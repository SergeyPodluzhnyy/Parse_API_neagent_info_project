This program loads and clears data from neagent.info in the database, the API allows you to update data and make views in JSON format.
HackerU Project
2020-10-12
<h1 align="center">Parse_API_neagent_info_project </h1>
<p align="center">
<img src="https://i.ibb.co/D48sCH6/Screenshot-155.jpg" width="80%"></p>

## About the project.
This program loads and clears data from neagent.info in the database, the API allows you to update data and make views in JSON format.


## How to install

### Windows 7:

- Create a folder on disk
- Save files to it
- Create a virtual environment:
    - Run CMD
    - cd <Path you project>
    - Create a virtual environment:<br>
    <pre>
        env\Scripts\activate   
        </pre>
- install all required python libraries in the new environment:<br>
    <pre>
        pip install -r req.txt
        </pre>

### Note:

- When the program is first launched, it creates the database  and CSV objects necessary for its operation.
- The program does not provide automatic data loading. Data is uploaded by the user.

### Program start-

- To run the program for execution, follow these:
        ```
        python app.py   
        ```
- At the end, the program displays the address to connect to:
    ``` 
    Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
    ``` 

### Program operation via the web interface:

**The program performs the following actions:**

- Load new data. page_num - this is the depth of loading data from the site neagent.info (number of pages starting from the first page)
        ```
        http://127.0.0.1:5000/api/load_new_data/<page_num>  
        ```
- Allows you to output a selection from the database (field/value)
        ```
        http://127.0.0.1:5000/api/select_from_db/<field>/<value>  
        ```       
        For example: 
            ```
            http://127.0.0.1:5000/api/select_from_db/district/ГМР  
            ```
- Displays apartments that have fallen in price           
        ```
        http://127.0.0.1:5000/api/cheap_aparts  
        ```
- Displays areas where apartments are becoming more expensive        
        ```
        http://127.0.0.1:5000/api/expensive_district  
        ```

### If data loading is successful, the browser will output JSON data
Else, an error message and error text will be displayed:
        ```
        {"status": "error", "error_text": "some error text"}
        ```    

## **Thank you for reading this far !**