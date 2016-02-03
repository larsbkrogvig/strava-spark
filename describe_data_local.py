import os
import re

def describe_data_local():

	# Initialize
	files = set([])
	athletes = None
	activity_types = set([])
	
	root = os.getcwd()+'/strava-activities'
	activity_pattern = '[0-9]+\-[0-9]+\-([\w]+).gpx'
	
	# Assume root directory has a set of subdirectories corresponding to athletes with files
	for (cur_root, cur_subdirs, cur_files) in os.walk(root):
	
		# Root level
		if cur_root == root:
			athletes = set(cur_subdirs)
	
		# Leaf level
		elif not cur_subdirs:
			files.update(set(cur_files))
	
	for fname in files:
		match = re.match(activity_pattern,fname)
		if not match:
			raise Exception('Encountered file with invalid file name')
		activity_types.add(match.group(1))
	
	return {'athletes': athletes, 'activity_types': activity_types}
