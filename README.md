# macro_data
Docker Container to pull frequently used macro data and save to SQL. The connection data to your postgres database should be stored in the file `pgres_url.txt`.
The python script will read it in from there. The Docker files are stored in `.devcontainer` with Docker Compose set up to make a postgresql container and a python container.

# Data 
Currently we pull the Greenbook forecast data, Federal Reserve BOG output gap estimates and the execess bond premium measures from Gilchrist and Zakrajsek. The Greenbookk data 
is stored in the `gb_forecasts` table with the schema
                         
    Column    |       Type       | Description               
--------------|------------------|---------------------------
variable     | text             | Greenbook variable name   
forecastdate | date             | Date of forecast          
valuedate    | date             | Quarterly forecasted date 
value        | double precision | Forecast Value            

Observations with `valuedate < forecastdate`, these are the real time estimates for that period. More info here
https://www.philadelphiafed.org/surveys-and-data/real-time-data-research/greenbook

The GZ excess bond premium data is downloaded from https://www.federalreserve.gov/econresdata/notes/feds-notes/2016/files/ebp_csv.csv
and is stored in the table `macro_data` with the schema 
  Column  |       Type       |
----------|------------------|
 date     | date             |
 variable | text             |
 value    | double precision |
