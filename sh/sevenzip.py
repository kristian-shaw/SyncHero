from .helpers import *
from .contextual_subprocess import ContextualSubprocess, SubprocessError

from pathlib import Path
from subprocess import CompletedProcess  # For type hinting


class SevenZip(ContextualSubprocess):
    @classmethod
    def configure(
        cls,
        context_pool_names: set[str],
        sevenzip_path: Path,
        destination_root_dir: Path,
    ) -> None:
        cls.raise_exception_if_class_configured()
        super().configure(context_pool_names, sevenzip_path, destination_root_dir)

    def raise_exception_if_proc_failed(self, proc: CompletedProcess) -> None:
        cmd_string = " ".join(map(str, proc.args))
        if proc.returncode == 0:
            return
        else:
            raise SevenZipError(cmd_string, proc.returncode, proc.stdout, proc.stderr)

    def extract(self) -> None:
        self.raise_exception_if_class_not_configured()
        self.raise_exception_if_context_not_set()
        cmd_args = [
            "x",
            "-bd",
            "-aoa",
            f"-o{str(self.get_extract_root_dir())}",
            str(self.get_destination_path()),
        ]
        extract_proc = self.run_subprocess(cmd_args)
        self.raise_exception_if_proc_failed(extract_proc)

    def is_archive_file(self) -> bool:
        self.raise_exception_if_class_not_configured()
        self.raise_exception_if_context_not_set()
        cmd_args = ["t", "-bd", "-p", str(self.get_destination_path())]
        test_proc = self.run_subprocess(cmd_args)
        return test_proc.returncode == 0

    def get_extract_root_dir(self) -> Path:
        self.raise_exception_if_class_not_configured()
        self.raise_exception_if_context_not_set()
        return Path(f"{str(self.get_destination_path())}.x")


class SevenZipError(SubprocessError):
    """
    Exception raised for errors that occur during 7zip operations.
    """

    def get_default_message(self, rc: int) -> str:
        return f"An error occured during the 7zip operation. Return code was {rc}"
