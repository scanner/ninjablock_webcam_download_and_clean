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
"""

# system imports
#
import argparse
import ConfigParser

# 3rd party imports
#
import dropbox
import arrow

####################################################################
#
def cl_arguments():
    """
    Return the command line argument parser.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file",
                        default = "/usr/local/etc/webcam_db.conf")
    parser.add_argument("--one_run", help="Instead of entering a loop "
                        "and running forever just do one run through the main "
                        "loop", action = "store_true")
    parser.add_argument("-n", "--dry_run", help="Do a dry run. Print out the "
                        "things we would do, but do not actually do them",
                        action = "store_true")
    return parser

####################################################################
#
def do_oauth_setup(sess, callback_url = None):
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
    print "Please visit this website and press the 'Allow' button, then hit 'Enter' here."
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

    <DATA DIR>/<yyyy>/<yyyy-mm-dd>/<yyyy.mm.dd-hh:mm:ss>.jpg

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
    year_dirs.sort(reverse = True)

    for year_dir in year_dirs:

        date_dirs = glob.glob(os.path.join(year_dir, "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"))

        date_dirs.sort(reverse = True)

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
    args = cl_arguments().parse_args()

    # Read in the config. We need this no matter what so we can get the
    # app key and app secret key.
    #
    config = ConfigParser.SafeConfigParser()
    config.read(args.config)

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
        with open(args.config, "wb") as configfile:
            config.write(configfile)
    else:
        sess.set_token(config.get("general", "access_token"),
                       config.get("general", "access_token_secret"))

    # Find the latest image file that we have already downloaded
    #
    img_file, date_dir, year_dir = find_latest_downloaded_file(args.dir)

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

    print "linked account:", client.account_info()
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

