import json
import shlex

from pathlib import Path


def format_section_str(section_parts):
    formatted_str = ""
    for part in section_parts:
        formatted_str += f"[{part}]"

    return formatted_str


def is_str_list(str_ling):
    return (
        bool(str_ling)
        and isinstance(str_ling, list)
        and all(isinstance(elem, str) for elem in str_ling)
    )


def is_json_file(file_path):
    try:
        with open(file_path, "r") as f:
            json.load(f)
    except:
        return False

    return True


def safe_str(value):
    try:
        value = str(value)
    except:
        return f"<uncastable {type(value)}>"

    return value


def del_dict_keys(dict_to_del, keys_to_del):
    for key in keys_to_del:
        if key in dict_to_del:
            del dict_to_del[key]


def get_context_hash(source_name, file_path):
    return str(hash(source_name + file_path))


def get_local_file_path_for_context(context, destination_root_dir):
    return Path(destination_root_dir) / context.source_name / context.file_path


def shell_escape_string(input):
    # Allows exceptions
    if isinstance(input, (list, tuple)):
        return list(map(shlex.quote, input))
    else:
        return shlex.quote(input)
