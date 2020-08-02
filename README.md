# dcmtag2table
Code to generate a Pandas DataFrame with a custom list DICOM tags from a folder containing DICOM files

## Usage

from dcmtag2table import dcmtag2table

list_of_tags = [
                "PatientID",
                "StudyInstanceUID",
                "SeriesInstanceUID",
                "SOPInstanceUID"
                ]
                
folder = "/media/felipe/easystore/Datasets/RSNA2019/mdai/epm/"

df = dcmtag2table(folder, list_of_tags)

