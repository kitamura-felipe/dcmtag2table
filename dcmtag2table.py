import pydicom
from tqdm import tqdm
import pandas as pd
import os
import shutil
import time
from typing import Set

def dcmtag2table(folder, list_of_tags):
    """
    # Create a Pandas DataFrame with the <list_of_tags> DICOM tags
    # from the DICOM files in <folder>

    # Parameters:
    #    folder (str): folder to be recursively walked looking for DICOM files.
    #    list_of_tags (list of strings): list of DICOM tags with no whitespaces.

    # Returns:
    #    df (DataFrame): table of DICOM tags from the files in folder.
    """
    list_of_tags = list_of_tags.copy()
    items = []
    table = []
    filelist = []
    print("Listing all files...")
    start = time.time()
    for root, dirs, files in os.walk(folder, topdown=False):
        for name in files:
            filelist.append(os.path.join(root, name))
    print("Time: " + str(time.time() - start))
    print("Reading files...")
    time.sleep(2)
    for _f in tqdm(filelist):
        try:
            ds = pydicom.dcmread(_f, stop_before_pixels=True)
            items = []
            items.append(_f)

            for _tag in list_of_tags:
                if _tag in ds:
                    items.append(ds.data_element(_tag).value)
                else:
                    items.append("Not found")

            table.append((items))
        except:
            print("Skipping non-DICOM: " + _f)

            
    list_of_tags.insert(0, "Filename")
    test = list(map(list, zip(*table)))
    dictone = {}

    for i, _tag in enumerate (list_of_tags):
        dictone[_tag] = test[i]

    df = pd.DataFrame(dictone)
    time.sleep(2)
    print("Finished.")
    return df

def replace_uids(df_in: pd.DataFrame, prefix = '1.2.840.1234.') -> pd.DataFrame:
    """
    # Maps the StudyInstanceUID, SeriesInstanceUID, and SOPInstanceUID
    # in a Pandas DataFrame with newly generated UIDs taking into account the 
    # Study/Series/SOP hierarchy. 
    # New columns with "Fake" prefix are created.

    # Parameters:
    #    df_in (Pandas DataFrame): DataFrame containing the three columns of UIDs
    #    prefix (str): string containing your particular prefix.

    # Returns:
    #    df (DataFrame): with three new columns containing the new UIDs
    """
    start = time.time()
    df = df_in.copy()
    
    list_of_tags = ["StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID" ]

    for _tag in list_of_tags:
        print("Reassigning " + _tag)
        if _tag not in df.columns:
            raise Exception('Tags StudyInstanceUID, SeriesInstanceUID, and SOPInstanceUID must be columns of the DataFrame')
        time.sleep(0.2)
        for _UID in tqdm(df[_tag].unique()):
            df.loc[df[_tag] == _UID, "Fake" + _tag] = pydicom.uid.generate_uid(prefix=prefix)
    print("Time: " + str(time.time() - start))
    return df

def age_string_to_int(age_str):
    """
    Convert an age string of format "NNL" to an integer.
    If 'L' is 'Y', remove it. If 'L' is any other letter, return 0.
    If 'L' is not present, return the number as is.
    
    :param age_str: Age in string format
    :return: Age as an integer
    """
    # Check if the last character is a letter
    if age_str[-1].isalpha():
        # If the letter is 'Y', remove it and convert to int
        if age_str[-1].upper() == 'Y':
            return int(age_str[:-1])
        # If the letter is not 'Y', return 0
        else:
            return 0
    # If the last character is not a letter, convert the whole string to int
    else:
        return int(age_str)


def list_files_in_directory(directory: str) -> Set[str]:
    """
    List all files in a directory and its subdirectories.
    
    :param directory: The directory to search for files.
    :return: A set of file paths.
    """
    file_paths = set()
    for root, _, files in tqdm(os.walk(directory)):
        for file in files:
            file_paths.add(os.path.join(root, file))
    return file_paths

def process_element(element, tag_values):
    """
    Process an individual DICOM element.
    If the element is a sequence, process each item recursively.
    Otherwise, add the tag and its value to the set.
    """
    if element.VR == "SQ":  # Sequence of items
        for item in element:
            if "PixelData" in item:
                del item.PixelData
            for sub_element in item.iterall():
                process_element(sub_element, tag_values)
    else:
        
        tag_values.add(f"{element.value}")

def iterate_dicom_tags(file_paths: list) -> Set:
    """
    Iterate over all DICOM tags in a given file, including sequences and nested sequences.
    """
    tag_values = set()
    for file_path in tqdm(file_paths):
        dicom_file = pydicom.dcmread(file_path)
        if "PixelData" in dicom_file:
            del dicom_file.PixelData
        for element in dicom_file.iterall():
            process_element(element, tag_values)

    return sorted(tag_values)

def save_set_to_file(data: Set[str], file_name: str):
    """
    Save the elements of a set to a file, each on a new line.

    :param data: Set of data to be saved.
    :param file_name: Name of the file to save the data.
    """
    with open(file_name, 'w') as file:
        for item in data:
            file.write(f"{item}\n")

def dump_unique_values(directory: str):
    print("Listing files")
    file_paths = list_files_in_directory(directory)
    print("Reading DICOM tags")
    dicom_tags = iterate_dicom_tags(file_paths)
    save_set_to_file(dicom_tags, "unique_values.txt")

def copy_files(df, column_name: str, folder2replace: str):
    """
    Copies files from source paths listed in a DataFrame to a destination path.
    The destination path is generated by replacing a specified folder name in the source path
    with the same folder name appended with '_filtered'.

    Parameters:
    df (pandas.DataFrame): A DataFrame containing file paths.
    column_name (str): The name of the column in the DataFrame where file paths are stored.
    folder2replace (str): The folder name in the path to be replaced with 'folder2replace_filtered'.

    The function iterates over each file path in the specified DataFrame column, replaces the specified 
    folder name in the path with 'folder2replace_filtered', creates the destination directory if it does 
    not exist, and then copies the file to the new location.
    """
    for source_path in tqdm(df[column_name]):
        # Replace 'upload' with 'upload_filtered' in the path
        destination_path = source_path.replace(folder2replace, folder2replace + "_filtered")

        # Create the destination directory if it doesn't exist
        destination_dir = os.path.dirname(destination_path)
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)

        # Copy the file
        shutil.copy2(source_path, destination_path)


def remove_if_tag_contains(df, tag: str, list2remove: list):
    """
    Filters out rows in a DataFrame based on whether a specified column (tag) contains any of the substrings
    in a given list. This function is case-insensitive.

    Parameters:
    df (pandas.DataFrame): The DataFrame to be filtered.
    tag (str): The name of the column in the DataFrame to check for substrings.
    list2remove (list): A list of substrings. If the 'tag' column contains any of these substrings, 
                        the corresponding row will be removed from the DataFrame.

    Returns:
    pandas.DataFrame: A DataFrame after removing rows where the 'tag' column contains any of the 
                      substrings in 'list2remove'.

    The function iterates over each substring in 'list2remove' and removes rows from the DataFrame 
    where the 'tag' column contains the substring. The search is case-insensitive.
    """
    for _substring in list2remove:
        df = df[~df[tag].str.contains(_substring, case=False)]
    return df
