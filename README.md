# dcmtag2table
Code to generate a Pandas DataFrame with a custom list DICOM tags from a folder containing DICOM files

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
df = df[df[Modality] == "CT"]

#Select only images with Slice Thickness of 1.0 mm or less
df = df[df[SliceThickness] <= 1.0]

```

To generate new UIDs for each unique UID ("StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID"):

```python
from dcmtag2table import replace_uids

df_out = replace_uids(df, prefix='your prefix here') # Example of prefix: "1.2.840.12345."
```
