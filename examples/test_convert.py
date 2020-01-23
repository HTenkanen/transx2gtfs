import transx2gtfs
import os

# transx2gtfs contains some data for testing
data_dir = transx2gtfs.get_path('test_data_dir')

# Save the output to current directory
current_dir = os.path.dirname(__file__)
outfp = os.path.join(current_dir, 'test_gtfs.zip')

# Do the conversion
transx2gtfs.convert(data_dir, outfp)