#!/usr/bin/env python
#
# File: $Id$
#
"""
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

Usage:
  webcam_download_rename_clean.py [options]
  webcam_download_rename_clean.py (-h | --help | --version)

Options:
  --version
  -h, --help                  Show this text and exit
  -c <file>, --config=<file>  Config [default: /usr/local/etc/webcam_db.conf]
  --one_run                   Instead of entering a loop  and running forever
                              just do one run through the main loop.
  -n, --dry_run               Do a dry run. Print out the things we would do,
                              but do not actually do them.
  --delete                    Do the step where we delete files older than a
                              certain date (see the '--expiry' option).
  --expiry                    The amount of time before we delete old files
                              in days. [default: 7]
  -f <dir>, --dropbox_folder=<dir>  The folder in the dropbox account that
                                    we are monitoring.
                                    [default: /Apps/Ninja Blocks/]
  -d <dir>, --dir=<dir>       Directory to download new images to
                              [default: /tmp/webcam]
  -i <s>, --interval=<s>      The interval in seconds between runs
                              [default: 30]
"""

# system imports
#
import glob
import ConfigParser
import re
import os
from time import sleep

# 3rd party imports
#
import dropbox
import arrow
from docopt import docopt

__version__ = "1.0.1"

# The regular expression to match which files we will rename to make
# sure we only bother re-naming ones we intend to.
#
FNAMES_TO_MATCH_re = re.compile(r'^(?P<weekday>\w\w\w), (?P<day>\d\d) '
                                '(?P<month>\w\w\w) (?P<year>\d\d\d\d) '
                                '(?P<hour>\d\d):(?P<minute>\d\d):'
                                '(?P<second>\d\d) (?P<tz>\w\w\w)\.jpg$')

# The regexp for a renamed image file.
#
DATE_FNAME_re = re.compile(r'^\d\d\d\d-\d\d-\d\dT\d\d_\d\d_\d\d-0000\.jpg$')

# the timestamp format used to parse and format our timestamp file names by
# arrow.
#
arrow_timestamp_fmt = "YYYY-MM-DDTHH_mm_ssZ"


####################################################################
#
def do_oauth_setup(sess, callback_url=None):
    """
    Construct a request token and print out the generated URL for the user to
    navigate to in their browser.

    After they authenticate the app to dropbox using the URL provided they are
    redirected back to the callback URL (which in this case does nothing.)

    Returns the 'access token' dropbox needs to grant this app access to
    dropbox. Definitely keep this private.

    Arguments:
    - `sess`: A dropbox session (already initialized with app_key and
              app_key_secret)
    - `callback_url`: a URL the user will be sent to after they authorize this
                      app to dropbox
    """

    # Get a request token. Ask the user to authorize linking their dropbox to
    # this app.
    #
    request_token = sess.obtain_request_token()
    url = sess.build_authorize_url(request_token,
                                   oauth_callback=callback_url)
    print "url:", url
    print ("Please visit this website and press the 'Allow' button, then hit "
           "'Enter' here.")
    raw_input()

    # To quote the tutorial from dropbox: "To avoid the hassle of setting up a
    # web server in this tutorial, we're just printing the URL and asking the
    # user to press the Enter key to confirm that they've authorized your
    # app. However, in real-world apps, you'll want to automatically send the
    # user to the authorization URL and pass in a callback URL so that the user
    # is seamlessly redirected back to your app after pressing a button."
    #
    # Assuming the user authorized this app we will get back an access token
    # that gives this app access to this dropbox account.
    #
    access_token = sess.obtain_access_token(request_token)
    return access_token


####################################################################
#
def find_latest_downloaded_file(data_dirname):
    """
    Given the name of the data directory find the name of the last downloaded
    file.

    We assume that the files have same naming convention:

    <DATA DIR>/<yyyy>/<yyyy-mm-dd>/<DATE_FNAME_re>

    Instead of using a directory walk all we need to do is find the latest year
    directory, and then the latest date directory within that year and then the
    latest date-time based file in that directory.

    If we can find no latest file we return (None, None, None)

    Arguments:
    - `data_dirname`: The data directory we are going to search for year
                      directories in.
    """
    year_dirs = glob.glob(os.path.join(data_dirname, "[0-9][0-9][0-9][0-9]"))
    if len(year_dirs) == 0:
        print "No year directories... "
        return None, None, None

    # Go backwards from the most recent year directory we find..
    #
    year_dirs.sort(reverse=True)

    for year_dir in year_dirs:

        date_dirs = glob.glob(
            os.path.join(year_dir,
                         "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]")
        )

        date_dirs.sort(reverse=True)

        if len(date_dirs) == 0:
            # This year has no date dirs.. goto the next (earlier) year dir
            #
            continue

        # Go backwards through the date directories in this year dir
        # looking for one that has at least one data file in it.
        #
        date_dirs.sort(reverse=True)

        for date_dir in date_dirs:

            # If this date dir has no data files goto to the next (earlier)
            # date dir
            #
            latest = glob.glob(os.path.join(date_dir, "*.jpg"))
            if len(latest) == 0:
                continue

            # However if this date dir has at least one data file in
            # it.. that data file is the latest one that has been
            # downloaded.
            #
            return (os.path.basename(latest[-1]), os.path.basename(date_dir),
                    os.path.basename(year_dir))

    # There are no downloaded data files..
    #
    return None, None, None


####################################################################
#
def get_dropbox_dir(client, db_folder):
    """
    Get the contents of a dropbox folder and its current hash.

    We only care about the file names in the folder and its current hash so we
    do not deal with the other metadata.

    XXX Since we are dealing with just images maybe we should restrict the list
        of files we return to ones that are images?

    Arguments:
    - `client`: A dropbox client
    - `db_folder`: The folder we want the contents of
    """
    folder_metadata = client.metadata(db_folder)
    files = []
    for f in folder_metadata['contents']:

        # Skip over directories
        #
        if f['is_dir']:
            continue

        files.append(os.path.basename(f['path']))

    return folder_metadata['hash'], files


####################################################################
#
def rename_dropbox_files(client, db_folder, files, dry_run):
    """
    Go through the files that are in the given dropbox folder, as passed in
    via the 'files' list. Filter out all the files that do not match our
    regular expression for the files we are going to rename, and then rename
    them in to sortable date based file names (the date we derive from their
    existing file name.)

    Arguments:
    - `client`: The dropbox client
    - `db_folder`: The dropbox folder we are operating in
    - `files`: The list of files from that dropbox folder
    """
    for fname in files:
        match = FNAMES_TO_MATCH_re.search(fname)

        # Skip over this loop if this file does not match our pattern.
        #
        if match is None:
            continue

        # The file name actually is alreay date based.. but it is just not
        # easily sortable so we want to convert it to a yyyy.mm.dd-hh:mm:ss
        # format for that reason.
        #
        d = arrow.get(str(fname[5:-4]), "DD MMM YYYY HH:mm:ss")
        new_fname = "%s.jpg" % d.format(arrow_timestamp_fmt)
        print "Renaming '%s' to '%s'" % (fname, new_fname)

        # If we are doing a dry-run skip to the next file here..
        #
        if dry_run:
            continue

        try:
            client.file_move(os.path.join(db_folder, fname),
                             os.path.join(db_folder, new_fname))
        except dropbox.rest.ErrorResponse, e:
            # It is okay if this file does not exist (means that it
            # was deleted before we could get to copying it..)
            #
            if e.status != 404:
                raise e
            else:
                print "File '%s' was deleted before we could rename it" % \
                    fname
    return


####################################################################
#
def download_new_files(client, db_folder, dest_dir, files, when, dry_run):
    """
    Download all of the files in the db_folder that are newer than 'when' that
    match our download pattern.

    Arguments:
    - `client`: Dropbox client
    - `db_folder`: Dropbox folder we are downloading from
    - `dest_dir`: The root destination directory to copy the files in to. The
                  sub-directory for the year, month, and day will be created
                  as necessary.
    - `files`: Use this list of files to decide what to download
    - `when`: An arrow timestamp. Download all files that were created after
              this timestamp.
    - `dry_run`: a boolean. If true then no actual actions are performed
    """

    # Going through the list of files only download ones that are after 'when'
    # and conform to our file name pattern.
    #
    for fname in files:
        # only download files that match our timestamp format. We want to force
        # the file names to be strings (not unicode) for safety of other
        # manipulations.
        #
        fname = str(fname)
        if DATE_FNAME_re.search(fname) is None:
            continue

        # Only download files that were created after 'when'
        #
        f_time = arrow.get(fname, arrow_timestamp_fmt)
        if f_time <= when:
            continue

        destination_dir = os.path.join(dest_dir, "%d" % f_time.year,
                                       f_time.format('YYYY-MM-DD'))
        destination_fname = os.path.join(destination_dir, fname)
        print "Downloading %s to %s" % (fname, destination_fname)

        # Wen doing a dry-run do not actually download the file.
        #
        if dry_run:
            continue

        # Make sure the destination directory exists and write out the file.
        #
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)

        try:
            f, metadata = client.get_file_and_metadata(os.path.join(db_folder,
                                                                    fname))
            out = open(destination_fname, 'wb')
            out.write(f.read())
            out.close()
        except dropbox.rest.ErrorResponse, e:
            # It is okay if this file does not exist (means that it
            # was deleted before we could get to copying it..)
            #
            if e.status != 404:
                raise e
            else:
                print "** File '%s' was deleted from the dropbox before we " \
                    "could download it" % fname
        else:
            print "** Done downloading %s" % fname
    return


####################################################################
#
def delete_old_files(client, db_folder, files, expiry, dry_run):
    """
    In the given dropbox folder delete all files that match our download file
    pattern whose creation time is older than 'now-expiry'

    NOTE: We only consider files that are in our 'files' array. We expect the
    calls to be close enough together that there is no real reason to make an
    additional call to Dropbox to get the list of files.

    And if we do not delete some files this run that we delete on the next run
    that is fine anyways.

    We are going to use the file's name as the determination of its creation
    time. We can do this because we have renamed all the files we care about to
    have this name format.

    This also makes sure that we are only deleting files that we actually want
    to delete.

    Arguments:
    - `client`: The dropbox client
    - `db_folder`: The dropbox folder we are deleting files from
    - `files`: The list of all the files we are going to consider
    - `expiry`: An arrow timestamp that the files must be older than in order
                to be considered for deletion.
    - `dry_run`: a boolean. If true then no actual actions are performed
    """
    # Going through the list of files only delete ones that are before 'expiry'
    # and conform to our file name pattern.
    #
    for fname in files:
        # only delete files that match our timestamp format. We want to force
        # the file names to be strings (not unicode) for safety of other
        # manipulations.
        #
        fname = str(fname)
        if DATE_FNAME_re.search(fname) is None:
            continue

        # Only download files that were created after 'when'
        #
        f_time = arrow.get(fname, arrow_timestamp_fmt)
        if f_time > expiry:
            continue
        print "** Deleting file '%s'" % fname
        if not dry_run:
            try:
                client.file_delete(os.path.join(db_folder, fname))
            except dropbox.rest.ErrorResponse, e:
                # It is okay if this file does not exist..
                #
                if e.status != 404:
                    raise e
    return


#############################################################################
#
def main():
    """
    If we are run in 'grant access mode' we do the necessary to get
    this app access to the user's dropbox, write the results to stable
    storage config file.

    Otherwise we assume we are being run in continuous fetch/update
    mode and we will basicallyl run through steps 1-3 of this script's
    purpose:

    o rename files to a sortable date based name
    o download new files to a specified directory
    o delete files from the dropbox folder that are older than a certain time

    """
    args = docopt(__doc__, version=__version__)
    dropbox_folder = args['--dropbox_folder']
    expiry = int(args['--expiry'])
    interval = int(args['--interval'])

    # Read in the config. We need this no matter what so we can get the
    # app key and app secret key.
    #
    config = ConfigParser.SafeConfigParser()
    config.read(args['--config'])

    # Setup our dropbox session. We use the app_key and app_secret we got from
    # the config file. This is a 'dropbox' app instead of an 'app folder' app.
    #
    sess = dropbox.session.DropboxSession(config.get("general", "app_key"),
                                          config.get("general", "app_secret"),
                                          "dropbox")

    # If the config does NOT have an 'access_token' then we need to get the
    # access token from the user and re-write the config file.
    #
    # XXX we should be encapsulating the writing of the file...
    #
    # XXX We should also be catching exceptions whenever we access the config
    #     file in case it is corrupted and things we expect to be there are not
    #     there.
    #
    if not config.has_option("general", "access_token"):
        access_token = do_oauth_setup(sess)
        config.set("general", "access_token", access_token.key)
        config.set("general", "access_token_secret", access_token.secret)
        with open(args['--config'], "wb") as configfile:
            config.write(configfile)
    else:
        sess.set_token(config.get("general", "access_token"),
                       config.get("general", "access_token_secret"))

    # Now that we have a session establish a client connection to dropbox and
    # begin our loop interogating the contents of this directory, renaming
    # files to a friendlier name for listingin order, downloading new files,
    # and removing files in this directory that are older than a certain date.
    #
    # NOTE: We only rename image files that follow a certain format, and we
    #       only delete files that are images that conform to the naming
    #       convention we rename the files to.
    #
    client = dropbox.client.DropboxClient(sess)

    # Dropbox returns a hash when we get the metadata for a directory that
    # tells us if anything in the directory has changed. This lets us quickly
    # know nothing has changed and skip the rest of the steps in one
    # loop. Since it starts out as 'None' the first time through will always do
    # all the steps.
    #
    last_dir_hash = None

    # And start our main loop that will go through the three tasks:
    # o rename files to date based names
    # o download all new files to the download directory
    # o remove files from dropbox that are older than the time period.
    #
    running = True

    while running:
        # Get the horizon in the past beyond which in the past we delete old
        # files
        #
        then = arrow.utcnow().replace(days=-expiry)

        # Find the latest image file that we have already downloaded
        #
        img_file, date_dir, year_dir = find_latest_downloaded_file(
            args['--dir']
        )

        # Convert the image file name in to a timestamp. I am going to be lazy
        # and just assume that the file name is in the proper format.
        #
        if img_file is not None:
            latest = arrow.get(img_file, arrow_timestamp_fmt)
        else:
            # Guess we better not have images older than the unix epoch..
            #
            latest = arrow.get(0)

        # Get the list of files and the hash directory we are watching. We can
        # skip the rest of this loop if the current directory hash is the same
        # as the last directory hash meaning nothing in this directory has
        # changed since the last time we asked.
        #
        try:
            cur_dir_hash, files = get_dropbox_dir(client,
                                                  dropbox_folder)
            if cur_dir_hash == last_dir_hash:
                print "** Skipping loop. No changes in folder '%s'" % \
                    dropbox_folder
                sleep(interval)
                continue

            # First step rename all the files that have the old file pattern.
            #
            print "** Renaming existing files"
            rename_dropbox_files(client, dropbox_folder, files,
                                 args['--dry_run'])

            # Second step, download all files that have appeared since the last
            # time we ran. We need to get the list of files again since we just
            # changed the contents of the directory by renaming files (and also
            # changing its hash)
            #
            print "** Downloading new files"
            last_dir_hash, files = get_dropbox_dir(client, dropbox_folder)
            download_new_files(client, dropbox_folder, args['--dir'], files,
                               latest, args['--dry_run'])

            # Finally (if '--delete' is set), delete files that are older a set
            # time (by default 7 days.)
            #
            if args['--delete']:
                print "** Deleteing old files"
                delete_old_files(client, dropbox_folder, files, then,
                                 args['--dry_run'])
        except dropbox.rest.ErrorResponse, e:
            # If we got anything but a 200 raise an exception (why did
            # we get a 200?)
            #
            if e.status not in (200, 500):
                print "** Wuh? Got dropbox.rest.ErrorResponse: %s" % str(e)
                raise e
            else:
                print "** huh. Got dropbox.rest.ErrorResponse: %s" % str(e)
        except dropbox.rest.RESTSocketError, e:
            # If we get a timeout, just continue on..
            #
            print "** Got errno from dropbox socket: %s" % repr(e)
            if e.errno == 60:
                print "** Connection to dropbox timed out."
            else:
                raise e

        # If we are doing a 'one run' then we immediate set 'running'
        # to false once we enter the loop so the loop will only run
        # once. Otherwise sleep..
        #
        if args['--one_run']:
            running = False
        else:
            print ("*** %s Done run. Sleeping for %d" %
                   (arrow.now().format('YYYY-MM-DD HH:mm:ss ZZ'), interval))
            sleep(interval)

    print "+*+* Exiting main loop"
    return

############################################################################
############################################################################
#
# Here is where it all starts
#
if __name__ == "__main__":
    main()

############################################################################
############################################################################
