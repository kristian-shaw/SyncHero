from sh.helpers import *
from sh.logger import Logger
from sh.metadata import MetadataManager, ContextError, ContextFileType
from sh.processes import ProcessManager, ProcessResult, ResultStatus
from sh.progress import ContextProgress, ProgressManager
from sh.rclone import RClone
from sh.sevenzip import SevenZip

from concurrent.futures import as_completed, wait
from pathlib import Path
from traceback import format_exception

# import argparse
import dataclasses
import json
import signal
import sys

logger = None
metadata_manager = None
progress_manager = None
process_manager = None
exiting = None


def main(arguments):
    # parser = argparse.ArgumentParser(
    # 	description=__doc__,
    # 	formatter_class=argparse.RawDescriptionHelpFormatter)
    # parser.add_argument(
    #     "path",
    #     help="Path to the songs folder.",
    #     type=str
    # )
    # args = parser.parse_args(arguments)

    cwd = Path.cwd()

    config_file_path = cwd / "config.json"
    config = None
    if config_file_path.is_file():
        try:
            with open(config_file_path, "r") as config_file:
                config = json.load(config_file)
        except Exception as error:
            print("ERROR: Could not open config.json")
            print(format_exception(None, error, error.__traceback__))
            sys.exit(1)
    else:
        print("ERROR: config.json not found")
        sys.exit(1)

    destination_root_dir = Path(config["settings"]["destination_dir"])
    if not destination_root_dir.is_absolute():
        destination_root_dir = Path.resolve(cwd / destination_root_dir)

    log_dir = None
    try:
        log_dir = Path(config["settings"]["log_dir"])
        if not log_dir.is_absolute():
            log_dir = Path.resolve(cwd / log_dir)
    except KeyError:
        log_dir = cwd

    global logger
    logger = Logger(log_dir, 50, 60)

    rclone_path = None
    try:
        rclone_path = Path(config["settings"]["rclone_path"])
        if not rclone_path.is_absolute():
            rclone_path = Path.resolve(cwd / rclone_path)
    except KeyError:
        rclone_path = cwd / "rclone.exe"

    rclone_config_path = None
    try:
        rclone_config_path = Path(config["settings"]["rclone_config_path"])
        if not rclone_config_path.is_absolute():
            rclone_config_path = Path.resolve(cwd / rclone_config_path)
    except KeyError:
        rclone_config_path = cwd / "rclone.conf"

    RClone.configure(
        {"file_operator"},
        rclone_path,
        destination_root_dir,
        rclone_config_path,
        config["sources"],
    )

    sevenzip_path = None
    try:
        sevenzip_path = Path(config["settings"]["7zip_path"])
        if not sevenzip_path.is_absolute():
            sevenzip_path = Path.resolve(cwd / sevenzip_path)
    except KeyError:
        sevenzip_path = cwd / "7z.exe"

    SevenZip.configure({"file_operator"}, sevenzip_path, destination_root_dir)

    MetadataManager.configure({"metadata"})
    global metadata_manager
    try:
        metadata_manager = MetadataManager(
            cwd / "metadata.json",
            config["settings"]["metadata_flush_loop_seconds"],
            True,
        )
    except Exception as error:
        print("ERROR: Could not setup metadata manager")
        print(format_exception(None, error, error.__traceback__))
        sys.exit(1)

    global progress_manager
    progress_manager = ProgressManager()

    remote_names = set([source["remote_name"] for source in config["sources"].values()])
    download_workers_per_remote = dict()
    source_remote_name_map = dict()
    for remote_name in remote_names:
        try:
            download_workers_per_remote[remote_name] = config["remote_configs"][
                remote_name
            ]["max_concurrent_downloads"]
        except KeyError:
            print(
                f"ERROR: max_concurrent_downloads not configured for remote_name: {remote_name}"
            )
            sys.exit(1)
    for source_name, source_config in config["sources"].items():
        remote_name = source_config["remote_name"]
        if remote_name not in download_workers_per_remote:
            if remote_name not in config["remote_configs"]:
                print(
                    f'ERROR: max_concurrent_downloads not configured for remote_name: {source_config["remote_name"]}'
                )
                sys.exit(1)
            download_workers_per_remote[remote_name] = config["remote_configs"][
                remote_name
            ]["max_concurrent_downloads"]
        source_remote_name_map[source_name] = remote_name

    global process_manager
    process_manager = ProcessManager(
        download_workers_per_remote,
        config["settings"]["max_concurrent_extracts"],
        config["settings"]["max_concurrent_deletes"],
        source_remote_name_map,
    )

    global exiting
    exiting = False
    exit_signal_handler = lambda signum, frame: process_manager.submit_exit_task(
        stop_processes
    )
    signal.signal(signal.SIGTERM, exit_signal_handler)
    signal.signal(signal.SIGINT, exit_signal_handler)

    try:
        print("INFO: Fetching file lists from sources")
        rclone = RClone()
        remote_files = dict()
        remote_file_count = 0
        for source_name in config["sources"].keys():
            if exiting:
                break
            rclone.set_context_source_name(source_name)
            remote_files[source_name] = []
            for file_info in rclone.fetch_file_info_list():
                if exiting:
                    break
                remote_file_count += 1
                remote_files[source_name].append(file_info)
            remote_files[source_name].sort(key=lambda item: item[0])
        del rclone

        remote_files = dict(sorted(remote_files.items()))
        print("INFO: Starting processes")
        metadata_manager.start_flush_metadata_process()
        contexts_in_progress = dict()
        for source_name, file_info_list in remote_files.items():
            if exiting:
                break
            for file_info in file_info_list:
                if exiting:
                    break
                remote_file_path = str(Path(file_info[0]))
                file_size = file_info[1]
                file_hash = file_info[2]
                metadata_manager.set_context_source_name(source_name)
                metadata_manager.set_context_file_path(remote_file_path)

                if (
                    metadata_manager.metadata_exists()
                    and not metadata_manager.error_exists()
                    and metadata_manager.get_remote_hash() == file_hash
                ):
                    remote_file_count -= 1
                else:
                    metadata_manager.initialize_metadata()
                    metadata_manager.set_remote_hash(file_hash)
                    metadata_manager.set_error_flag(
                        ContextError.CANCELLED, True
                    )  # Assume cancelled until proven otherwise
                    context = metadata_manager.get_context()
                    context_path = context.as_path(include_source=True)
                    contexts_in_progress[context_path] = ContextProgress(
                        context,
                        metadata_manager.get_metadata(),
                        set(),
                        [],
                        {remote_file_path},
                        False,
                    )

                    thread_rclone = RClone()
                    thread_sevenzip = SevenZip()
                    download_file_future = process_manager.submit_download_task(
                        context,
                        context,
                        download_file,
                        context,
                        thread_rclone,
                        thread_sevenzip,
                    )
                    contexts_in_progress[context_path].futures.add(download_file_future)

        metadata_manager.free_context()
        progress_manager.set_total_files(remote_file_count)
        logger.set_total_files(remote_file_count)
        logger.start_drawing_progress()

        print("INFO: Waiting for processes")
        while len(process_manager.get_futures()) > 0:
            if exiting:
                break
            try:
                # Process only the first future, then fall back to while loop and get a fresh iterator that includes new futures
                for future in as_completed(
                    process_manager.get_futures(), timeout=10
                ):  # On TimeoutError, catch it and simply fall back to while loop for regular exiting check
                    root_context = future.root_context
                    process_manager.remove_future(future)
                    metadata_manager.set_context(root_context)
                    root_context_path = metadata_manager.context.as_path(
                        include_source=True
                    )
                    contexts_in_progress[root_context_path].futures.discard(future)
                    contexts_in_progress[root_context_path].files_to_process.discard(
                        root_context.file_path
                    )
                    if future.cancelled():
                        contexts_in_progress[root_context_path].cancelled = (
                            True  # Consider the status of a root archive cancelled if any tasks for its members are cancelled
                        )
                    else:
                        for result in future.result():
                            context = dataclasses.replace(future.context)  # Make a copy
                            match result.status:
                                case ResultStatus.DONE:
                                    pass  # Nothing to do
                                case ResultStatus.EXTRACT_NEEDED:
                                    metadata_manager.set_file_type(
                                        ContextFileType.ARCHIVE
                                    )
                                    contexts_in_progress[root_context_path].metadata = (
                                        metadata_manager.get_metadata()
                                    )
                                    thread_sevenzip = SevenZip()
                                    try:
                                        extract_archive_file_future = (
                                            process_manager.submit_extract_task(
                                                context,
                                                root_context,
                                                extract_archive_file,
                                                context,
                                                thread_sevenzip,
                                                root_context,
                                            )
                                        )
                                        contexts_in_progress[
                                            root_context_path
                                        ].futures.add(extract_archive_file_future)
                                        contexts_in_progress[
                                            root_context_path
                                        ].files_to_process.add(context.file_path)
                                    except RuntimeError:
                                        pass  # Ignore thread pool shutting down
                                case (
                                    ResultStatus.DOWNLOAD_FAILED
                                    | ResultStatus.EXTRACT_FAILED
                                ):
                                    contexts_in_progress[
                                        root_context_path
                                    ].errors.append(result.error)
                                    if result.status == ResultStatus.DOWNLOAD_FAILED:
                                        metadata_manager.set_error_flag(
                                            ContextError.DOWNLOAD_FAILED, True
                                        )
                                    if result.status == ResultStatus.EXTRACT_FAILED:
                                        metadata_manager.set_error_flag(
                                            ContextError.EXTRACT_FAILED, True
                                        )
                                    contexts_in_progress[root_context_path].metadata = (
                                        metadata_manager.get_metadata()
                                    )
                                    for context_future in contexts_in_progress[
                                        root_context_path
                                    ].futures:  # Cancel all further processing for the root context at the first failure
                                        context_future.cancel()
                    if (
                        len(contexts_in_progress[root_context_path].files_to_process)
                        == 0
                    ):  # True for completed or failed downloads of non-archives as well as fully processed or failed root archives
                        metadata_manager.set_error_flag(
                            ContextError.CANCELLED,
                            contexts_in_progress[root_context_path].cancelled,
                        )
                        contexts_in_progress[root_context_path].metadata = (
                            metadata_manager.get_metadata()
                        )
                        metadata_manager.free_context()
                        register_processed_file(contexts_in_progress[root_context_path])
                    break  # Fall back to while loop
            except TimeoutError:
                pass
        print("INFO: Finished processing results")
    except Exception as error:
        message = "ERROR: Caught exception"
        logger.submit_output(message)
        logger.write_to_log_file(
            f"{message}\n"
            + "\n".join(format_exception(None, error, error.__traceback__))
        )
    finally:
        process_manager.submit_exit_task(stop_processes)
        print("INFO: Main thread waiting for exit process")
        wait([process_manager.get_exit_future()])
        print(
            f"INFO: {progress_manager.get_processed_files()}/{progress_manager.get_total_files()} files have been successfully processed"
        )


def download_file(context, rclone, sevenzip):
    result = ProcessResult(ResultStatus.DONE, context, context, None)
    try:
        rclone.set_context(context)
        rclone.download()
        rclone.free_context()
        sevenzip.set_context(context)
        if sevenzip.is_archive_file():
            result.status = ResultStatus.EXTRACT_NEEDED
        sevenzip.free_context()
    except Exception as e:
        result.status = ResultStatus.DOWNLOAD_FAILED
        result.error = e
    finally:
        rclone.free_context()
        sevenzip.free_context()
    return [result]


"""extract_archive_file
Parameters:
context - the context map for the file to be extracted
sevenzip - 7z instance with context already set
root_context - the context map for the top level archive in the chain
Returns: [ProcessResult]
"""


def extract_archive_file(context, sevenzip, root_context=None):
    if root_context is None:
        root_context = context.copy()
    try:
        sevenzip.set_context(context)
        destination_root_dir = sevenzip.get_destination_root_dir()
        archive_extract_dir = sevenzip.get_extract_root_dir()
        archive_result = ProcessResult(
            ResultStatus.DONE, context, root_context, None
        )  # Always included in return value as the last element
        sevenzip.extract()
    except Exception as e:
        archive_result.status = ResultStatus.EXTRACT_FAILED
        archive_result.error = e
        sevenzip.free_context()
        return [archive_result]
    results = []
    for dirpath, dirnames, filenames in archive_extract_dir.walk():
        for filename in filenames:
            full_file_path = dirpath / filename
            result_file_path = full_file_path.parts[
                len(destination_root_dir.parts) + 1 : len(full_file_path.parts)
            ]  # Get the path relative from the source directory
            try:
                sevenzip.set_context_file_path(str(result_file_path))
                extracted_file_result = ProcessResult(
                    ResultStatus.DONE, sevenzip.get_context(), root_context, None
                )
                if sevenzip.is_archive_file():
                    extracted_file_result.status = ResultStatus.EXTRACT_NEEDED
            except Exception as e:
                archive_result.status = ResultStatus.EXTRACT_FAILED
                archive_result.error = e
                sevenzip.free_context()
                return [
                    archive_result
                ]  # Minimise further processing for the root archive by returning only the first error
            results.append(extracted_file_result)
    sevenzip.free_context()
    return results + [archive_result]


def register_processed_file(context_progress):
    global logger
    global progress_manager
    if context_progress is None or context_progress.cancelled:
        processed_files, failed_files = progress_manager.register_failed_file()
    elif len(context_progress.errors) == 0:
        processed_files, failed_files = progress_manager.register_processed_file()
    else:
        context = context_progress.context
        metadata = context_progress.metadata
        if metadata.file_type == ContextFileType.ARCHIVE:
            message = f"ERROR: Errors occured while processing archive file. source_name: {context.source_name}, file_path: {context.file_path}"
        else:
            message = f"ERROR: Errors occured while downloading file. source_name: {context.source_name}, file_path: {context.file_path}"

        processed_files, failed_files = progress_manager.register_failed_file()
        logger.submit_output(message)
        tracebacks = "\n".join(
            [
                "\n".join(format_exception(None, error, error.__traceback__))
                for error in context_progress.errors
            ]
        )
        logger.write_to_log_file(f"{message}\n" + tracebacks)
    logger.set_done_files(processed_files + failed_files)


def stop_processes():
    global logger
    global metadata_manager
    global process_manager
    global exiting
    # Ignore additional calls to this function
    if not exiting:
        exiting = True
        print("INFO: Exit process started")
        if logger.is_drawing():
            logger.stop_drawing_progress()
        metadata_manager.stop_flush_metadata_process()
        pools = process_manager.get_download_pools() + [
            process_manager.get_extract_pool(),
            process_manager.get_delete_pool(),
        ]
        print("INFO: Waiting for current processes to finish...")
        for pool in pools:
            if pool is not None:
                pool.shutdown(wait=True, cancel_futures=True)
        metadata_manager.flush_metadata()
        process_manager.get_exit_pool().shutdown(
            wait=False, cancel_futures=True
        )  # Main thread waits for this


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
