import pydicom
from tqdm import tqdm
import pandas as pd
import os
import time

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
