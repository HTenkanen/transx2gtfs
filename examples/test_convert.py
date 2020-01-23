import transx2gtfs
import os

def temp_output_filepath():
    import tempfile
    import os
    temp_dir = tempfile.TemporaryDirectory()
    temp_fp = os.path.join(
        temp_dir.name, os.path.basename(
            tempfile.mktemp(
                suffix=".zip")))
    return temp_fp

# transx2gtfs contains some data for testing
data_dir = transx2gtfs.get_path('test_data_dir')

# Save the output to current directory
current_dir = os.path.dirname(__file__)
outfp = os.path.join(current_dir, temp_output_filepath())
print(outfp)
# Do the conversion
#transx2gtfs.convert(data_dir, outfp)