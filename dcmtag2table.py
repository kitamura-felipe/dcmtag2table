import pydicom
from pydicom import Dataset
from pydicom.dataset import FileMetaDataset
from pydicom import config
from tqdm import tqdm, tqdm_notebook
import pandas as pd
import os
import shutil
import time
from typing import Set
from datetime import datetime
from joblib import Parallel, delayed

# Relax the integer parsing rules
config.enforce_valid_values = False

non_phi_ct_dicom_tags = [ # These are required tags for CT. Make sure to change this when working with other modalities (MR, CR, US)
    'PixelData',
    'SeriesNumber',          # Number of the series within the study
    'AcquisitionNumber',     # Number identifying the single continuous gathering of data
    'InstanceNumber',        # Number identifying the image
    'Modality',              # Type of equipment that created the image (CT for computed tomography)
    'Manufacturer',          # Manufacturer of the equipment
    'SliceThickness',        # Thickness of the slice in mm
    'SpacingBetweenSlices'   # the distance between two adjacent slices in millimeters, measured from the center of each slice to the center of the other slice
    'KVP',                   # Peak kilovoltage output of the X-ray tube used
    'DataCollectionDiameter',# Diameter of the region from which data were collected
    'SoftwareVersions',      # Software versions of the equipment
    'ReconstructionDiameter',# Diameter within which the reconstruction is performed
    'GantryDetectorTilt',    # Tilt of gantry with respect to the table
    'TableHeight',           # Height of the table
    'RotationDirection',     # Direction of rotation of the source around the patient (CW or CCW)
    'ExposureTime',          # Time of X-ray exposure in ms
    'XRayTubeCurrent',       # X-ray tube current in mA
    'Exposure',              # Dose area product in mGy*cm²
    'FilterType',            # Type of filter used
    'GeneratorPower',        # Power of the generator used to make the exposure in kW
    'FocalSpots',            # Size of the focal spot in mm
    'ConvolutionKernel',     # Description of the convolution kernel or kernels used for the reconstruction
    'PatientPosition',       # Position of the patient relative to the imaging equipment space
    'SliceLocation',         # Location of the slice
    'ImagePositionPatient',  # Position of the image frame in patient coordinates
    'ImageOrientationPatient', # Orientation of the image frame in patient coordinates
    'SamplesPerPixel',        # Number of samples (colors) in the image
    'PhotometricInterpretation', # Photometric interpretation
    'Rows',                   # Number of rows in the image
    'Columns',                # Number of columns in the image
    'PixelSpacing',           # Physical distance between the center of each pixel
    'BitsAllocated',          # Number of bits allocated for each pixel sample
    'BitsStored',             # Number of bits stored for each pixel sample
    'HighBit',                # Most significant bit for pixel sample data
    'PixelRepresentation',    # Data representation of the pixel samples
    'WindowCenter',           # Window center for display
    'WindowWidth',            # Window width for display
    'RescaleIntercept',       # Value to be added to the rescaled slope intercept
    'RescaleSlope'            # Slope for pixel value rescaling
]

required_mg_dicom_tags = [
    # General Series Module
    "Modality",
    "SeriesNumber",

    # General Equipment Module
    "Manufacturer",

    # General Image Module
    "ImageType",
    "InstanceNumber",
    "AcquisitionNumber",
    "SeriesDescription",
    "StudyDescription",

    # Image Pixel Module
    "SamplesPerPixel",
    "PhotometricInterpretation",
    "Rows",
    "Columns",
    "BitsAllocated",
    "BitsStored",
    "HighBit",
    "PixelRepresentation",
    "PixelData",

    # -- DX Image Module
    "KVP",
    "DistanceSourceToDetector",
    "ExposureTime",
    "XRayTubeCurrent",
    "Exposure",
    "CassetteOrientation",
    "CassetteSize",
    "ExposuresOnPlate",

    # -- Mammography Image Module
    "BodyPartExamined",
    "PixelSpacing",
    "FilterMaterial",
    "FilterType",
    "CompressionForce",
    "ViewPosition",
    "PatientOrientation",
    "PresentationLUTShape",

    # -- Newly added items from fields that were missing:
    "DistanceSourcetoDetector",
    "DistanceSourcetoPatient",
    "EstimatedRadiographicMagnificationFactor",
    "X-rayTubeCurrent",
    "ExposureinuAs",
    "ImagerPixelSpacing",
    "Grid",
    "FocalSpots",
    "AnodeTargetMaterial",
    "BodyPartThickness",
    "RelativeX-rayExposure",
    "PositionerType",
    "PositionerPrimaryAngle",
    "DetectorConditionsNominalFlag",
    "DetectorTemperature",
    "DetectorType",
    "DetectorID",
    "ImageLaterality",
    "ImagesinAcquisition",
    "SamplesperPixel",
    "PixelPaddingValue",
    "QualityControlImage",
    "BurnedInAnnotation",
    "PixelIntensityRelationship",
    "PixelIntensityRelationshipSign",
    "WindowCenter",
    "WindowWidth",
    "RescaleIntercept",
    "RescaleSlope",
    "RescaleType",
    "ImplantPresent",
    "LossyImageCompression",
    "Sensitivity",
    "AcquisitionDeviceProcessingCode",
    "ImagesInAcquisition",
    "BreastImplantPresent",
    "RelativeXRayExposure",
    "SpecificCharacterSet",
    "DetectorConfiguration",
    "DetectorDescription",
    "SOPClassUID",
    "ManufacturerModelName",
    "DistanceSourceToPatient",
    "PositionerSecondaryAngle",
    "DetectorActiveShape",
    "DetectorActiveDimensions",
    "FieldOfViewOrigin",
    "FieldOfViewRotation",
    "FieldOfViewHorizontalFlip",
    "PixelAspectRatio",
    "FieldOfViewShape",
    "GridPeriod",
    "PartialView",
    "PartialViewDescription",
    "FilterThicknessMinimum",
    "ExposureInuAs",                # (0018, 1153)
    "FilterThicknessMaximum",
    "ExposureControlMode",
    "Laterality",
    "ExposureControlModeDescription",
    "ExposureStatus",
    "EthnicGroup"
]


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
            ds = pydicom.dcmread(_f, stop_before_pixels=True, force=True)
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
import os
import time
import pydicom
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed


def _read_dicom_tags(filepath, list_of_tags):
    """
    Helper function to read a single DICOM file
    and extract the requested tags.
    Returns a list [filepath, tag1, tag2, ...] or None on failure.
    """
    try:
        ds = pydicom.dcmread(filepath, stop_before_pixels=True, force=True)
        row = [filepath]
        for tag in list_of_tags:
            value = ds.data_element(tag).value if tag in ds else "Not found"
            row.append(value)
        return row
    except Exception:
        # If it's not a valid DICOM or can't be read, return None
        return None


def dcmtag2table_parallel(folder, list_of_tags, max_workers=4):
    """
    Create a Pandas DataFrame with the <list_of_tags> DICOM tags
    from the DICOM files in <folder>, in parallel.

    Parameters:
        folder (str): folder to be recursively walked looking for DICOM files.
        list_of_tags (list of str): list of DICOM tags with no whitespaces.
        max_workers (int): number of parallel processes to use.

    Returns:
        df (pd.DataFrame): table of DICOM tags from the files in folder.
    """
    list_of_tags = list_of_tags.copy()
    filelist = []

    print("Listing all files...")
    start = time.time()
    for root, dirs, files in os.walk(folder):
        for name in files:
            filelist.append(os.path.join(root, name))
    print("Time for listing: {:.2f} seconds".format(time.time() - start))

    # Prepare for parallel processing
    print("Reading DICOM tags in parallel...")
    start_read = time.time()

    rows = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit jobs
        futures = {executor.submit(_read_dicom_tags, f, list_of_tags): f for f in filelist}

        # Collect results with a progress bar
        for future in tqdm(as_completed(futures), total=len(futures)):
            fpath = futures[future]
            result = future.result()
            if result is None:
                # If reading failed, print a message (optional)
                print(f"Skipping non-DICOM or unreadable: {fpath}")
            else:
                rows.append(result)

    print("Time for reading: {:.2f} seconds".format(time.time() - start_read))

    # Build the DataFrame
    # Prepend "Filename" to the list_of_tags so it aligns with the row format
    column_names = ["Filename"] + list_of_tags
    df = pd.DataFrame(rows, columns=column_names)
    df = df.sort_values(by=['Filename'], ascending=True)
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
            df.loc[df[_tag] == _UID, "fake" + _tag] = pydicom.uid.generate_uid(prefix=prefix)
    print("Time: " + str(time.time() - start))
    return df

def replace_uids_parallel_joblib(df_in: pd.DataFrame, prefix='1.2.840.1234.', n_jobs=-1) -> pd.DataFrame:
    """
    Parallel method using joblib to map the UID columns in a DataFrame.
    New columns with "fake" prefix are created.

    Parameters:
        df_in (pd.DataFrame): DataFrame with the UIDs
        prefix (str): prefix for generating new UIDs
        n_jobs (int): number of parallel jobs (-1 = all cores)

    Returns:
        df (pd.DataFrame)
    """
    start = time.time()
    df = df_in.copy()
    
    list_of_tags = ["StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID"]
    
    # Basic validation
    for tag in list_of_tags:
        if tag not in df.columns:
            raise ValueError(
                "DataFrame must have StudyInstanceUID, SeriesInstanceUID, and SOPInstanceUID columns"
            )
    
    def make_mapping(tag):
        """Generate the mapping dict for a single column."""
        unique_vals = df[tag].unique()
        # tqdm here if you'd like to monitor progress
        mapping = {val: pydicom.uid.generate_uid(prefix=prefix) for val in unique_vals}
        return tag, mapping

    # Generate mapping dicts in parallel
    results = Parallel(n_jobs=n_jobs)(
        delayed(make_mapping)(tag) for tag in tqdm(list_of_tags, desc="Generating UID maps")
    )
    
    # Apply mapping to the DataFrame
    for tag, mapping in results:
        df[f"fake_{tag}"] = df[tag].map(mapping)
        
    df = df.sort_values(by=['Filename'], ascending=True)
    
    print("Time: {:.2f} seconds".format(time.time() - start))
    return df
    
def replace_ids(df_in: pd.DataFrame, prefix: str, start_pct=1, start_study=1) -> pd.DataFrame:
    """
    # Maps the PatientID, StudyID
    # in a Pandas DataFrame with newly generated IDs taking into account the 
    # Patient/Study/Series/SOP hierarchy. 
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
            df.loc[df[_tag] == _UID, "fake_" + _tag] = pydicom.uid.generate_uid(prefix=prefix)

    list_of_tags = ["PatientID", "StudyID", "AccessionNumber" ]

    for _tag in list_of_tags:
        print("Reassigning " + _tag)
        if _tag not in df.columns:
            raise Exception('Tags PatientID, StudyID, AccessionNumber must be columns of the DataFrame')
        time.sleep(0.2)
        
        if _tag == "PatientID":
            counter = start_pct
            for _UID in df[_tag].unique():
                df.loc[df[_tag] == _UID, "fake_" + _tag] = counter
                counter += 1
        else:
            counter = start_study
            for _UID in df["StudyInstanceUID"].unique():
                df.loc[df["StudyInstanceUID"] == _UID, "fake_" + _tag] = counter
                counter += 1        


        if _tag == "PatientID":
            last_patient = counter
        elif _tag == "StudyID":
            last_study = counter
            
    print("Time: " + str(time.time() - start))
    print("Last Patient: " + str(last_patient))
    print("Last Study: " + str(last_study))
    return df

def replace_ids_parallel_joblib(df_in: pd.DataFrame, prefix: str, start_pct=1, start_study=1, n_jobs=-1) -> pd.DataFrame:
    """
    # Maps the PatientID, StudyID
    # in a Pandas DataFrame with newly generated IDs taking into account the 
    # Patient/Study/Series/SOP hierarchy. 
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

    # Basic validation
    for tag in list_of_tags:
        if tag not in df.columns:
            raise ValueError(
                "DataFrame must have StudyInstanceUID, SeriesInstanceUID, and SOPInstanceUID columns"
            )
    
    def make_mapping(tag):
        """Generate the mapping dict for a single column."""
        unique_vals = df[tag].unique()
        # tqdm here if you'd like to monitor progress
        mapping = {val: pydicom.uid.generate_uid(prefix=prefix) for val in unique_vals}
        return tag, mapping

    # Generate mapping dicts in parallel
    results = Parallel(n_jobs=n_jobs)(
        delayed(make_mapping)(tag) for tag in tqdm(list_of_tags, desc="Generating StudyInstanceUID, SeriesInstanceUID, and SOPInstanceUID maps")
    )
    
    # Apply mapping to the DataFrame
    for tag, mapping in results:
        df[f"fake_{tag}"] = df[tag].map(mapping)

    #list_of_tags = ["PatientID", "StudyID", "AccessionNumber" ]
    print("Assigning new PatientIDs.")
    unique_patients = df["PatientID"].unique()
    patient_mapping = {
        pat_id: i + start_pct for i, pat_id in enumerate(unique_patients)
    }
    df["fake_PatientID"] = df["PatientID"].map(patient_mapping)

    print("Assigning new StudyIDs.")
    unique_studies = df["StudyInstanceUID"].unique()
    study_mapping = {
        study_uid: i + start_study for i, study_uid in enumerate(unique_studies)
    }
    df["fake_StudyID"]        = df["StudyInstanceUID"].map(study_mapping)
    print("Assigning new AccessionNumbers.")
    df["fake_AccessionNumber"] = df["StudyInstanceUID"].map(study_mapping)

    df = df.sort_values(by=['Filename'], ascending=True)

    last_patient = start_pct + len(unique_patients)
    last_study = start_study + len(unique_studies)
    print("Time: " + str(time.time() - start))
    print("Last Patient: " + str(last_patient))
    print("Last Study: " + str(last_study))
    return df

def allow_list(in_path: str, out_path: str, list_of_tags: list, start_pct=1, start_study=1):
    """
    Processes DICOM files to anonymize and retain only a specified list of tags, saving the modified files to a new location.

    This function reads DICOM files from a specified input path, anonymizes patient and study identifiers, and creates new DICOM files that include only a predefined list of DICOM tags, along with newly anonymized tags. The new files are saved to a specified output path, organized by StudyID.

    Parameters:
    - in_path (str): The file path to the directory containing the original DICOM files.
    - out_path (str): The file path to the directory where the modified DICOM files will be saved.
    - list_of_tags (list): A list of DICOM tags that should be retained in the new DICOM files.
    - start_pct (int, optional): Starting value for the pseudonymization counter for PatientID and PatientName. Defaults to 1.
    - start_study (int, optional): Starting value for the pseudonymization counter for StudyID and AccessionNumber. Defaults to 1.

    Returns:
    - DataFrame: A pandas DataFrame containing the mappings between original and fake identifiers for all processed DICOM files.

    Note:
    The function uses `dcmtag2table` to extract specified DICOM tags into a DataFrame and `replace_ids` to anonymize identifiers. It requires `pydicom` for DICOM file handling and `os` for file path operations. Progress is tracked using `tqdm`.

    The anonymization process assigns new values to PatientID, PatientName, StudyID, AccessionNumber, StudyInstanceUID, SeriesInstanceUID, and SOPInstanceUID, while retaining specified clinical tags. Certain fixed values are assigned to PatientBirthDate, PatientSex, PatientAge, and StudyDate, StudyTime, and the ProtocolName is cleared.
    """
    
    phi_dicom_tags = [
        'PatientID',             # Unique identifier for the patient
        'PatientName',           # Name of the patient
        'PatientBirthDate',      # Birth date of the patient
        'PatientSex',            # Sex of the patient
        'PatientAge',
        'ReferringPhysicianName',# Name of the referring physician
        'StudyID',               # ID of the study
        'AccessionNumber',
        'DeviceSerialNumber',    # Serial number of the device
        'StudyInstanceUID',      # Unique identifier for the study
        'StudyDate',             # Date of study initiation
        'StudyTime',             # Time of study initiation
        'SeriesInstanceUID',     # Unique identifier for the series
        'SOPInstanceUID',     # Unique identifier for the series
        'ProtocolName',          # Name of the protocol used for the series
    ]

    df = dcmtag2table_parallel(in_path, phi_dicom_tags, max_workers=16)

    df = replace_ids_parallel_joblib(df, prefix="1.2.840.12345.", start_pct=start_pct, start_study=start_study)
    counter = 0
    for index, row in tqdm(df.iterrows(), total=len(df)):
        #if counter < 52820:
        #    continue
        #counter += 1
        original_file_path = row['Filename']
        
        # Read the original DICOM file
        original_ds = pydicom.dcmread(original_file_path, force=True)
        #print(original_ds.file_meta)

        
        # Create a new DICOM dataset
        new_ds = Dataset()
        new_ds.file_meta = FileMetaDataset()
        try:
            new_ds.file_meta = original_ds.file_meta
        except:
            print("No file_meta found. Skipping file.")
            continue
        
        if 'TransferSyntaxUID' in original_ds.file_meta:
            tsyntax = original_ds.file_meta.TransferSyntaxUID
            new_ds.is_little_endian = tsyntax not in [
                "1.2.840.10008.1.2",  # Implicit VR Little Endian
            ]
            new_ds.is_implicit_VR = tsyntax in [
                "1.2.840.10008.1.2",  # Implicit VR Little Endian
            ]
        else:
            # Se o Transfer Syntax não estiver presente, usar valores comuns
            new_ds.is_little_endian = True  
            new_ds.is_implicit_VR = False  

        
        # Copy only the predefined tags from the original to the new dataset
        for tag in list_of_tags:
            if tag in original_ds:
                new_ds.add(original_ds[tag])


        new_ds.PatientID = str(int(row['fake_PatientID'])).zfill(6)
        new_ds.PatientName = str(int(row['fake_PatientID'])).zfill(6)
        new_ds.PatientBirthDate = "19190828"
        new_ds.PatientSex = row['PatientSex']
        new_ds.PatientAge = row['PatientAge']
        new_ds.StudyID = str(int(row['fake_AccessionNumber'])).zfill(6)
        new_ds.AccessionNumber = str(int(row['fake_AccessionNumber'])).zfill(6)
        new_ds.StudyInstanceUID = row['fake_StudyInstanceUID']
        new_ds.SeriesInstanceUID = row['fake_SeriesInstanceUID']
        new_ds.SOPInstanceUID = row['fake_SOPInstanceUID']
        new_ds.file_meta.MediaStorageSOPInstanceUID = row['fake_SOPInstanceUID']
        new_ds.ProtocolName = ""
        new_ds.StudyDate = "20250228"
        new_ds.SeriesDate = new_ds.StudyDate
        new_ds.ContentDate = new_ds.StudyDate
        new_ds.AcquisitionDate = new_ds.StudyDate
        new_ds.StudyTime = "000000"
        new_ds.SeriesTime = new_ds.StudyTime
        new_ds.ContentTime = new_ds.StudyTime
        new_ds.AcquisitionTime = new_ds.StudyTime

        # Construct the new file path based on StudyID

        new_file_path = os.path.join(out_path, new_ds['StudyID'].value, new_ds['SOPInstanceUID'].value + ".dcm")
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(new_file_path), exist_ok=True)
        
        # Save the new DICOM file
        new_ds.save_as(new_file_path)
            
    return df

def _process_single_row(
    index, 
    row, 
    out_path: str, 
    list_of_tags: list
):
    """
    Process a single row from the DataFrame: read the original DICOM,
    copy only certain tags, anonymize / replace IDs, and write out the new DICOM.
    """
    original_file_path = row['Filename']

    try:
        # Read original
        original_ds = pydicom.dcmread(original_file_path, force=True)
    except Exception as e:
        print(f"Failed to read DICOM {original_file_path} - {e}")
        return

    # Create new dataset
    new_ds = Dataset()
    new_ds.file_meta = FileMetaDataset()

    # Copy file_meta if present
    if hasattr(original_ds, 'file_meta'):
        new_ds.file_meta = original_ds.file_meta

    # Check TransferSyntaxUID
    if 'TransferSyntaxUID' in original_ds.file_meta:
        tsyntax = original_ds.file_meta.TransferSyntaxUID
        new_ds.is_little_endian = tsyntax not in ["1.2.840.10008.1.2"]  # Implicit VR LE
        new_ds.is_implicit_VR  = tsyntax in  ["1.2.840.10008.1.2"]     
    else:
        new_ds.is_little_endian = True
        new_ds.is_implicit_VR  = False

    # Copy only the tags we want
    for tag in list_of_tags:
        if hasattr(original_ds, tag):
            new_ds.add(original_ds[tag])

    # Use your anonymized IDs from the row
    new_ds.PatientID        = str(int(row['fake_PatientID'])).zfill(6)
    new_ds.PatientName      = str(int(row['fake_PatientID'])).zfill(6)
    new_ds.PatientBirthDate = "19190828"
    new_ds.PatientSex       = row.get('PatientSex', 'O')  # safe fallback
    new_ds.PatientAge       = row.get('PatientAge', '000Y')

    new_ds.StudyID          = str(int(row['fake_AccessionNumber'])).zfill(6)
    new_ds.AccessionNumber  = str(int(row['fake_AccessionNumber'])).zfill(6)

    new_ds.StudyInstanceUID      = row['fake_StudyInstanceUID']
    new_ds.SeriesInstanceUID     = row['fake_SeriesInstanceUID']
    new_ds.SOPInstanceUID        = row['fake_SOPInstanceUID']
    new_ds.file_meta.MediaStorageSOPInstanceUID = row['fake_SOPInstanceUID']

    # Clear or set fixed fields
    new_ds.ProtocolName = ""
    new_ds.StudyDate    = "20250228"
    new_ds.SeriesDate   = new_ds.StudyDate
    new_ds.ContentDate  = new_ds.StudyDate
    new_ds.AcquisitionDate = new_ds.StudyDate
    new_ds.StudyTime    = "000000"
    new_ds.SeriesTime   = new_ds.StudyTime
    new_ds.ContentTime  = new_ds.StudyTime
    new_ds.AcquisitionTime = new_ds.StudyTime

    # Construct new file path
    new_file_path = os.path.join(
        out_path, 
        new_ds.StudyID, 
        new_ds.SOPInstanceUID + ".dcm"
    )

    # Ensure directory exists
    os.makedirs(os.path.dirname(new_file_path), exist_ok=True)

    # Save
    try:
        new_ds.save_as(new_file_path)
    except Exception as e:
        print(f"Failed to save DICOM {new_file_path} - {e}")


def allow_list_parallel(
    in_path: str, 
    out_path: str, 
    list_of_tags: list, 
    start_pct=1, 
    start_study=1,
    max_workers=8
):
    """
    Processes DICOM files to anonymize and retain only a specified list of tags,
    saving the modified files to a new location, **in parallel**.
    """
    # 1) Extract DICOM tags in parallel (assuming your function already does this)
    phi_dicom_tags = [
        'PatientID','PatientName','PatientBirthDate','PatientSex','PatientAge',
        'ReferringPhysicianName','StudyID','AccessionNumber','DeviceSerialNumber',
        'StudyInstanceUID','StudyDate','StudyTime','SeriesInstanceUID','SOPInstanceUID','ProtocolName'
    ]
    df = dcmtag2table_parallel(in_path, phi_dicom_tags, max_workers=16)

    # 2) Replace IDs in parallel (assuming your function already does this)
    df = replace_ids_parallel_joblib(df, prefix="1.2.840.12345.", start_pct=start_pct, start_study=start_study)
    
    # 3) Final DICOM read/modify/write in parallel
    tasks = (
        delayed(_process_single_row)(index, row, out_path, list_of_tags)
        for index, row in df.iterrows()
    )

    # Optional: wrap in tqdm for a progress bar
    Parallel(n_jobs=max_workers)(
        tqdm(tasks, total=len(df), desc="Processing DICOMs")
    )

    return df

def age_string_to_int(age_str: str) -> int:
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

def no_phi_age(age_str: str) -> str:
    """
    Convert an age string of format "NNL" to a HIPAA compliant
    age.
    Patients older than 89Y will be assigned to 90Y
    
    :param age_str: Age in string format
    :return: Age in string format never older than 90Y
    """
    age_int = age_string_to_int(age_str)
    if age_int > 89:
        age_int = 90
    return str(age_int) + "Y"

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
        dicom_file = pydicom.dcmread(file_path, force=True)
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

def dump_unique_values(directory: str, output="unique_values.txt"):
    print("Listing files")
    file_paths = list_files_in_directory(directory)
    print("Reading DICOM tags")
    dicom_tags = iterate_dicom_tags(file_paths)
    save_set_to_file(dicom_tags, output)

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

def get_folder_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in tqdm(filenames):
            fp = os.path.join(dirpath, f)
            if os.path.exists(fp):
                total_size += os.path.getsize(fp)
    return total_size

def append_to_csv(file_path, data_dict):
    # Create a DataFrame from the dictionary
    new_row = pd.DataFrame([data_dict])

    # Check if the file exists
    if os.path.exists(file_path):
        # Read existing data
        df = pd.read_csv(file_path)
        # Concatenate the new data
        df = pd.concat([df, new_row], ignore_index=True)
    else:
        # Use the new row as the DataFrame
        df = new_row
    
    # Save to CSV
    df.to_csv(file_path, index=False)
    
def get_metrics(folder: str, output_file: str):
    list_of_tags = [
                "PatientID",
                "StudyInstanceUID",
                "SeriesInstanceUID",
                "SOPInstanceUID",
                "Modality",
                "PatientSex",
                "PatientAge"
                ]
    df = dcmtag2table(folder, list_of_tags)

    summary = {
        "Timestamp": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
        "Number of files": len(df),
        "Batch Size Bytes": get_folder_size(folder),
        "Number of patients": len(df['PatientID'].unique()),
        "Number of studies": len(df['StudyInstanceUID'].unique()),
        "Number of series": len(df['SeriesInstanceUID'].unique()),
        "Number of MRs": len(df.drop_duplicates('StudyInstanceUID')[df.drop_duplicates('StudyInstanceUID')['Modality'] == 'MR']),
        "Number of CTs": len(df.drop_duplicates('StudyInstanceUID')[df.drop_duplicates('StudyInstanceUID')['Modality'] == 'CT']),
        "Number of USs": len(df.drop_duplicates('StudyInstanceUID')[df.drop_duplicates('StudyInstanceUID')['Modality'] == 'US']),
        "Number of CRs": len(df.drop_duplicates('StudyInstanceUID')[df.drop_duplicates('StudyInstanceUID')['Modality'] == 'CR']),
        "Number of DXs": len(df.drop_duplicates('StudyInstanceUID')[df.drop_duplicates('StudyInstanceUID')['Modality'] == 'DX']),
        "Percentage of male": len(df.drop_duplicates('PatientID')[df.drop_duplicates('PatientID')['PatientSex'] == 'M']) / len(df.drop_duplicates('PatientID')),
    }
    append_to_csv(output_file, summary)
    return summary

