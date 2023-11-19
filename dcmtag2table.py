import pydicom
from tqdm import tqdm
import pandas as pd
import os
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
    for root, _, files in os.walk(directory):
        for file in files:
            file_paths.add(os.path.join(root, file))
    return file_paths

def read_dicom_tags(file_paths: Set[str]) -> Set[str]:
    """
    Read DICOM tags from the given file paths and accumulate their values.

    :param file_paths: Set of file paths to process.
    :return: A set of DICOM tag values.
    """
    tag_values = set()

    for file_path in file_paths:
        try:
            dicom_file = pydicom.dcmread(file_path)
            for tag in dicom_file.dir():
                try:
                    value = getattr(dicom_file, tag)
                    tag_values.add(f"{value}")
                except AttributeError:
                    pass  # Skip if the attribute is not present
        except pydicom.errors.InvalidDicomError:
            pass  # Skip non-DICOM files

    return tag_values

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
    file_paths = list_files_in_directory(directory)
    dicom_tags = read_dicom_tags(file_paths)
    save_set_to_file(dicom_tags, "unique_values.txt")

