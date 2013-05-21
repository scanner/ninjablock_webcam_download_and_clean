ninjablock_webcam_download_and_clean
====================================

A simple python script for downloading webcam images deposited in to Dropbox by a ninjablock.

This is a script that accomplishes a set of tasks related to a directory in a
dropbox account that is being populated by the ninjablock cloud service from a
webcam attached to a ninjablock.

The three tasks are:

o rename files in to a consistent naming pattern based on the file's creation
  date. The pattern is based on the ISO timestamp format.

o download all new files in to a specified directory (after renaming)

o remove files from the dropbox directory after they have been there for a
  certain amount of time. Because I plan on having something like this running
  for months if not years I only want to keep, say, the last week in the
  dropbox. Everything else is preserved when I download it to a local
  directory.

It could be argued that the third task should be its own script but I do not
want to deal with all the session and other bits of overhead in a separate
script at this time.

NOTE: We depend on the python modules listed in the 'requirements.txt'
file. All hail 'pip install -r ./requirements.txt' + virtualenvs!
