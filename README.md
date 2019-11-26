# Vendor Performance Grid

Scripts for creating csv files that can be used to update the Vendor Performance grid.

## Requirements

All required packages are in the requirements.txt file. There is also an included environment.yml file for setting up a conda environment.

### PaceUtils
Requires that the paceutils package to be installed. Can be found at http://github.com/whatscottcodes/paceutils.

Requires a SQLite database set up to the specifications in https://github.com/whatscottcodes/database_mgmt

## Use

Run script to generate csv files with relevant information. Can have start date and end date passed as parameters, if they are not passed will run for the last quarter.

