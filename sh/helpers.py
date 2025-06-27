# import inspect
import json
import shlex
# import os

from pathlib import Path

# import py7zr
# import rarfile
# import tarfile
# import zipfile

def format_section_str(section_parts):
	formatted_str = ""
	for part in section_parts:
		formatted_str += f"[{part}]"

	return formatted_str

def is_str_list(str_ling):
	return bool(str_ling) and isinstance(str_ling, list) and all(isinstance(elem, str) for elem in str_ling)

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

# def get_file_type(file_path):
# 	if py7zr.is_7zfile(file_path):
# 		return "py7zr"
# 	elif rarfile.is_rarfile(file_path):
# 		return "rarfile"
# 	elif tarfile.is_tarfile(file_path):
# 		return "tarfile"
# 	elif zipfile.is_zipfile(file_path):
# 		return "zipfile"
# 	else:
# 		return "other"

# def filter_for_top_level(file_path_list):
# 	results = set()
# 	for file_path in file_path_list:
# 		first, second = os.path.split(file_path)
# 		while first != "":
# 			first, second = os.path.split(first)
# 		if second not in results:
# 			results.add(second)
# 	return results

# def delete_file_if_exists(path_to_delete):
# 	try:
# 		os.remove(path_to_delete)
# 	except FileNotFoundError as e:
# 		pass

def del_dict_keys(dict_to_del, keys_to_del):
	for key in keys_to_del:
		if key in dict_to_del:
			del dict_to_del[key]

def get_context_hash(source_name, file_path):
	return hash(source_name + file_path)

def get_local_file_path_for_context(context, destination_root_dir):
	return Path(destination_root_dir) / context["source_name"] / context["file_path"]

def dictify(obj): # Credit due to SO answer: https://stackoverflow.com/a/72955055
    if isinstance(obj, (int, float, str, bool)):
        return obj
    if isinstance(obj, (list, tuple)):
        return list(map(dictify, obj))
    return {
		#{
		#	_type: <type name>
		#	<attribute name>: <child dict or primitive value>
		#   <optional extra attribute(s)>: <child dict or primitive value>
		#}
		"_type": type(obj).__name__, # Name for the type of the value
		**{k: dictify(v) for k, v in obj.__dict__.items()} # Recurse for each attribute at the current level, reusing the attribute names
	}

def shell_escape_string(input):
	# Allows exceptions
	if isinstance(input, (list, tuple)):
		return list(map(shlex.quote, input))
	else:
		return shlex.quote(input)

# def strip_filename_extensions_from_path(file_path):
# 	if os.path.isdir(file_path):
# 		return file_path
# 	else:
# 		basename = os.path.basename(file_path)
# 		dot_index = basename.index(".")
# 		return file_path[:len(file_path) - (len(basename) - dot_index)]
