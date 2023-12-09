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

To dump unique values from DICOM tags:

```python
from dcmtag2table import dump_unique_values

dump_unique_values('/mnt/d/exames/HeadCT/')

```

To generate new UIDs for each unique UID ("StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID"):

```python
from dcmtag2table import replace_uids

df_out = replace_uids(df, prefix='your prefix here') # Example of prefix: "1.2.840.12345."
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

