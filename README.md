# dcmtag2table
Code to generate a Pandas DataFrame with a custom list of DICOM tags from a folder containing DICOM files.
It can be used to perform series selection, generation of new UIDs, and dumping of unique values of DICOM tags to expedite manual PHI checks.

Thanks to Errol Colak for envisioning the algorithm to dump unique values. 

## Usage

```python
#Import package to read metadata from DICOM files
from dcmtag2table import dcmtag2table

#Define the tags we want to read from DICOM files
list_of_tags = [
                "PatientID",
                "StudyInstanceUID",
                "SeriesInstanceUID",
                "SOPInstanceUID",
                "SliceThickness",
                "Modality"
                ]

#Define the folder where DICOM files are stored
folder = "/media/felipe/easystore/Datasets/RSNA2019/mdai/epm/"

#Read tags from DICOM files into a Pandas Dataframe
df = dcmtag2table(folder, list_of_tags)

#Select only CTs
df = df[df["Modality"] == "CT"]

#Select only images with Slice Thickness of 1.0 mm or less
df = df[df["SliceThickness"] <= 1.0]

```

To pseudonymize DICOM files, use allow_list():

```python
from dcmtag2table import replace_ids, allow_list

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

df = allow_list_parallel("/mnt/d/dataset_jpr/defaced_epm/", 
           "/mnt/c/dataset_jpr/deid_unifesp/",
           non_phi_ct_dicom_tags,
           start_pct=1,
           start_study=1,
           max_workers=16)

# The output is a Pandas DataFrame correlating the real and fake IDs and UIDs.
# If you delete (or don't save) that DataFrame, then the data is anonymized, since the is no way to reidentify the studies.
# The pseudonymized files will be saved in "/mnt/c/dataset_jpr/deid_unifesp/".

```

To dump unique values from DICOM tags:

```python
from dcmtag2table import dump_unique_values

dump_unique_values_parallel('/mnt/d/exames/HeadCT/')

```

To generate new UIDs for each unique UID ("StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID"):

```python
from dcmtag2table import replace_uids

df_out = replace_uids(df, prefix='your prefix here') # Example of prefix: "1.2.840.12345."
```

To generate new IDs (PatientID, StudyID, and AccessionNumber) and UIDs for each unique UID ("StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID"):

```python
from dcmtag2table import replace_ids

df_out = replace_uids(df, prefix='your prefix here', start_pct=1, start_study=1) # Example of prefix: "1.2.840.12345."
```

The `remove_if_tag_contains` function can be particularly useful for filtering DICOM datasets based on the values of specific DICOM tags. This function allows you to remove rows from a DataFrame where a specified DICOM tag column contains any of the substrings from a given list, which is helpful in cleaning or organizing DICOM data. Here's an example:

```python
from dcmtag2table import remove_if_tag_contains

# Substrings to remove - Series Description that contains 'T1' or 'T2'
list2remove = ['T1', 'T2']

# Usage of remove_if_tag_contains
# Assuming 'SeriesDescription' is the DICOM tag we're interested in
filtered_df = remove_if_tag_contains(df, 'SeriesDescription', list2remove)

# Display the filtered DataFrame
print(filtered_df)
```


The `copy_files` function is designed to automate the process of copying files from one location to another, with the ability to modify a part of the directory path during the copy. This can be particularly useful for organizing files into different directories based on certain criteria. After filtering the DataFrame with the function above, you can create a copy of the dataset only with the desired files. Here's a simple usage example:

```python
from dcmtag2table import copy_files

# Specify the folder name to replace in the path
folder2replace = 'dataset1'

# Usage of copy_files
copy_files(df, 'Filename', folder2replace)

# This will copy files to paths like '/data/dataset1_filtered/study1/series1/image1.dcm', etc.
```

The `get_metrics` function is designed to automate the process of summarizing the number of files, patients, studies, series, and total byte size of a dataset. Here's a simple usage example:

```python
from dcmtag2table import get_metrics

# Specify the folder namevwhere the DICOM files are saved.
folder = 'dataset1'

# Usage of get_metrics
get_metrics(folder, "transfer_logs.csv")

# This will generate a dictionary as output. It will also append the dictionary to transfer_logs.csv.

Output:

{'Timestamp': '12/28/2023, 07:07:40',
 'Number of files': 1000,
 'Batch Size Bytes': 526754688,
 'Number of patients': 1,
 'Number of studies': 1,
 'Number of series': 8,
 'Number of MRs': 0,
 'Number of CTs': 1,
 'Number of USs': 0,
 'Number of CRs': 0,
 'Number of DXs': 0,
 'Percentage of male': 1.0}

```
