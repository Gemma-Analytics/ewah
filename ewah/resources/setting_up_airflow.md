Author: soltanianalytics

# Setting up apache airflow

This is a short guide on how to set up airflow with LocalExecutor on a local machine for development and on a remote server running Ubuntu 18.

This is not a complete and definite guide - it is just one of many ways you can make airflow run. However, it is a detailed description of _one_ specific way you might run airflow. Many deployment decisions were made implicitly, so please use this as a get-started guide rather than a comprehensive tell-all guide!

## Getting started: Requirements

Before you get started, have the following ready:
- An empty, remote git repository (ideally initialized with a Python-specific `.gitignore` file)
- Access to a cloud service like AWS (I will use AWS terminology, but any cloud service would work)
- Running Linux on you computer (this guide should also work with Mac OS or Windows Subsystem for Linux, but is not tested and may deviate in the details)


## Setting up airflow locally

This step-by-step guidance was developed on a machine running Ubuntu 18 that had previously installed:
- [Python](https://www.python.org/downloads/) 3.7 (as of 2020-08-16, I have not yet switched to Python 3.8 since there had been some compatibility issues with airflow and dbt)
- many libraries that may be required to run airflow (you may receive some errors you need to fix; you might also want to check out the setting up of airflow on a remote server step to get a list of libraries to install with `apt-get`)
- [git](https://git-scm.com/)
- [Atom](https://atom.io/) as text editor (any text editor should do)

Follow the following steps:
- create a PostgreSQL database with AWS RDS (or any other service, or just locally) and save the connection details
  - ideally test if you can connect to the database manually
  - this database will be the metadata database
  - if you don't use it for any other purpose, it need not be particularly powerful or large
  - you _could_ also use the built-in sql database and get started without any external database, but I generally like to develop my airflow deployments as close to the production environment as possible, which means running Ubuntu and using a remote PostgreSQL database as metadata database
- open a terminal and navigate to a folder of your choice; we will go with `~/dev`
- clone the git repository (we will call it `airflow_git`); going forward we will thus call it `~/dev/airflow_git`
- create a subfolder called `airflow_home` and navigate to it (`mkdir airflow_home && cd airflow_home`)
  - This is not strictly required, but I like to have an explicit subfolder for airflow in case other analytics-related code is added to the repository
  - --> the airflow home folder is `~/dev/airflow_git/airflow_home`
- create a new virtual environment in the airflow home folder
  - you need to have the `virtualenv` package installed; just use pip: `pip3.7 install --upgrade virtualenv`
  - then: `python3.7 -m venv env`
  - note: depending on your Python installation, you may nee to use `pip3.7`/`python3.7`, or `pip3`/`python3`, or just `pip`/`python` - since I have multiple version of Python installed, I always need to use the formermost
- activate the virtual environment with `source env/bin/activate`
- add three environment variables
  - `export AIRFLOW_HOME="~/dev/airflow_git/airflow_home"` to tell your installation where your airflow-related code will be
  - `export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://[username]:[password]@[host]:[port]/[database_name]"` to tell airflow the connection details to your airflow metadata database (note: the password may need to be url readable, i.e. special signs need to be url encoded (e.g. `!` becomes `%21`); see [here](https://www.urlencoder.io/python/) how to do it in Python3)
  - `export AIRFLOW__CORE__REMOTE_LOGGING=False` to overwrite any present or future remote logging configuration meant for production deployments
  - to make life for your future self easier, add these in your virtualenv activation script (located at `.../airflow_home/env/bin/activate`)
    - add the three export statements below the line `export PATH`
    - add the lines `unset AIRFLOW_HOME`, `unset AIRFLOW__CORE__SQL_ALCHEMY_CONN`, and unset AIRFLOW__CORE__REMOTE_LOGGING somewhere in the deactivate function, outside of any if statement (e.g. directly above or below the `unset VIRTUAL_ENV` statement that should be in there somewhere)
    - this way, you don't need to manually export these values in the future when developing locally, while simultaneously not having to worry about them if you deactivate the venv to work on something different
- install airflow with `pip install --upgrade apache-airflow[all]`
  - note that you must now use `pip` instead of `pip3.7` etc. as you are now in a Python3.7 (or whatever version you are using) virtual environment
  - instead of [all], you may alternatively only install the components you really need - see [here](https://airflow.apache.org/docs/stable/installation.html#extra-packages) for a current list of components
  - this installation may take a few minutes...
  - as mentioned previously, if you get errors check if you might need to install some libraries with `apt-get` that I otherwise install on a freshly created remote server when setting up airflow
- run `airflow initdb` to set up your metadata database and the `airflow.cfg` file
  - if the config file was not created, check if you have exported the environment variables above!
- add `airflow_home/logs` to your `.gitingore` file
- make a few adjustments to the `airflow_home/airflow.cfg` file
  - replace all instances of `~/dev/airflow_git/airflow_home` with `$AIRFLOW_HOME`
  - set the executor to `executor = LocalExecutor`
  - (optional) set `load_examples = False` unless you want to load and see the example DAGs later
  - take the `fernet_key`, save it locally, remove it from the config file (don't just outcomment it!), outcomment the rest of the line in the config file, and set it as an environment variable (including adding it to your virtuelenv activate script, as with the other three above) - *this is very important because this key is used to encrypt and decrypt the connection credentials in the airflow metadata database*, and you don't want to have this key in a remote git repository! Instead, treat it like a password that you use for services like 1Password etc.
    - --> the line `fernet_key = [fernet_key]` becomes `# fernet_key = `
    - the env var is `export AIRFLOW__CORE__FERNET_KEY=[fernet_key]`
    - make sure to export it in your commandline as well as adding it in your activate script or alternatively deactivate and reactivate the virtualenv after adding it to your activate script (you deactivate a virtualenv with the command `deactivate`)
  - if you wish to use the email feature of airflow (highly encouraged!), adjust the smtp settings accordingly; if you use gmail you can make the following changes in the config
    - `smtp_host = smtp.gmail.com`
    - `smtp_port = 587`
    - outcomment `smtp_mail_from`
    - use environment variables to set `smtp_user` and `smtp_password` to avoid committing this information to a git repository, just like was the case with the fernet_key (env var names: `AIRFLOW__SMTP__SMTP_USER` and `AIRFLOW__SMTP__SMTP_PASSWORD`)
    - I use app passwords - [here](https://support.google.com/accounts/answer/185833?hl=en) is a guide on how to create a 16-character app password that you can use as `smtp_password` along with your email as `smtp_user`
  - congrats, now your config file should be production ready (using `LocalExecutor`)!
- open a second terminal, navigate to `~/dev/airflow_git/airflow_home`, and activate the virtualenv with `source env/bin/activate`
- in one terminal, run `airflow webserver`, and in the other, run `airflow scheduler`
- open a web browser of your choice and navigate to `localhost:8080` - congrats, airflow should now be running locally on your machine!
  - note that you may locally see the example DAGs even if you set `load_examples = False` earlier, as they may have been loaded into the metadata database before changing the config - but if you turn one on and want to run it, they won't work unless you set `load_examples = True` in your config first (fyi: you may need to restart the airflow processes after making a config change)
- finally, you might also want to add `airflow_home/airflow-webserver.pid` to your `.gitignore`

## Setting up airflow on a remote server

Chances are, if you wish to run airflow in production, that you want to run it on a server somewhere. This server might be on-premise or in a cloud, such as AWS. There are many ways to run airflow. You can run it with a SaaS like `http://astronomer.io/` or you can run it on your own server. You can run it on a single server with `LocalExecutor` or you can run it exploiting distributed computing with the `CeleryExecutor` or `KubernetesExecutor`. This guide is intended to explain the manual set up of airflow on a single server running Ubuntu 18, using `LocalExecutor`.

I will assume that you went through all steps in the previous section and committed all your changes to your remote git repository called `airflow_git`. Now it's time to deploy that repository to a production machine.

Follow the following steps:
- First, provision a server running Ubuntu 18, e.g. on AWS EC2 (I recommend having at the very least 4GB of RAM, otherwise airflow will not be happy - and more is always better :) )
- Second, provision a PostgreSQL database as airflow metadata database, e.g. on AWS RDS
  - you can use the same database you use as DWH, however I would recommend creating a separate _database_ on your database server because airflow will use the `public` schema for its metadata tables
- connect to the server via SSH - if you want to use a GUI that works the same on Windows and Linux, I can recommend [PuTTY](https://www.putty.org/); the below are steps to take if you use PuTTY
  - Putty uses `.ppk` keys for authentication (if you connect to your server with a private key, which I would highly recommend) - AWS will, however, give you a `.pem` private key -> you can use the tool `PuTTYgen` which is installed alongside `PuTTY` on Windows or use the commandline to convert your key from one format to the other (see [here](https://aws.amazon.com/premiumsupport/knowledge-center/convert-pem-file-into-ppk/) for an instruction, with the caveat that I recommend not using `sudo` if possible)
  - enter the public ip or dns in the host name field (e.g. `ec2-1-123-123-123.eu-central-1.compute.amazonaws.com`)
  - the port is usually `22`
  - in the category `Connection`, go to the `Data` page (on the left side) and enter your username as `Auto-login username` (AWS EC2: `ubuntu`)
  - in the `Connection->SSH->Auth` page, at the bottom, browse to the location of your private key (`.ppk` format)
  - in the `Connection->SSH->Tunnels` page, enter `8080` as source port, `localhost:8080` as destination, and _then_ click `Add` -> this will allow you to see the airflow webserver GUI once it runs on the server
  - finally, back on the `Session` page, enter an appropriate name in the `Saved Sessions` field, click `Save` (otherwise you'd need to repeat all of the above everytime you wish to connect), and then click `Open`
  - the first time you connect to a server, you may receive a `PuTTY Security Alert`, click `Accept`
- once connected, first update the linux installation and install Python
  - see the file `ubuntu_airflow_setup.txt` for a list (current on 2020-08-22)
  - `sudo apt-get update && sudo apt-get -y dist-upgrade`
  - install various packages: `sudo apt-get install -y alien libaio1 openjdk-11-jre-headless build-essential libbz2-dev libssl-dev libreadline-dev libsqlite3-dev tk-dev libpng-dev libfreetype6-dev nginx libmysqlclient-dev python3-dev libevent-dev freetds-dev git-core libsasl2-dev gcc python-dev libkrb5-dev libffi-dev libpq-dev` - these libraries were found to be important via trial and error by myself over time; some of there may not be required (anymore)... but it does work
  - install an appropriate version of Python (as of writing, I discourage the use of Python 3.8 but recommend the latest 3.7, which was 3.7.9) - note that I only use `sudo` where required
  - the only package you should install is `virtualenv` - everything else should be installed _within_ a virtual environment
- next, clone the previously created airflow git repository from your remote git repository
  - If you want to make both the initial and future deployments easy, you may want to create an ssh key and add it to your remote git account [(you can use this guide)](https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent); alternatively use https now (command: `git clone [url]`)
- from now on, this guide assumed you clone the `airflow_git` repository set up locally in the previous section
- navigate to `airflow_git`, i.e. `cd airflow_git`
- create a virtual environment in the same spot you did on your own local machine, which was here in this case: `python3.7 -m venv env`
- I run my airflow processes in tmux sessions
  - create a new `tmux` session with `tmux new -s airflow` (tmux will run the processes on the machine and will continue to run it after you quit your SSH session; moreover, you will be able to acces the same tmux session from a new SSH connection; `airflow` in this case is just a name for the session, albeit appropriately descriptive)
  - split this session window into three panes using (`CTRL+b`, then `%`) or (`CTRL+b`, then `"`) - everything below, do in all three panes (you can switch panes by pressing `CTRL+b` and then using the arrow keys)
    - activate the virtual environment: `source env/bin/activate`
    - add all relevant environment variables, i.e.
      - `export AIRFLOW_HOME="~/dev/airflow_git/airflow_home"`
      - `export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://[username]:[password]@[host]:[port]/[database_name]"`
      - `export AIRFLOW__CORE__FERNET_KEY=[fernet_key]`
      - optional (for emails): `export AIRFLOW__SMTP__SMTP_USER=...` and `export AIRFLOW__SMTP__SMTP_PASSWORD=...`
      - (obivously, you don't want to set the remote logging config in production)
  - in one of the panes, install airflow (make sure the virtual environment is active): `pip install --upgrade apache-airflow[all]`
  - run the `airflow upgradeb` command _once_ - if successful, your metadata database is now set up and airflow is ready to go!
  - run `airflow webserver` in one paner and `airflow scheduler` in another
  - the third pane is an optional pane, ready and set up in case you need to debug something on short notice
- open the airflow UI in your browser by visiting `localhost:8080` if you set up tunneling earlier or `[ip]:8080` if you opened the port to the world (or whitelisted your ip for that port)
  - check out the official documentation for web UI security and authentication options: `https://airflow.apache.org/docs/stable/security.html`
  - personally, I prefer to not open any ports other than 22 to anything and use SSH tunnelling whenever I need to access the airflow web UI
- finally, add your connections in the airflow web UI and turn on your DAGs
- congrats, you have successfully set up airflow!
