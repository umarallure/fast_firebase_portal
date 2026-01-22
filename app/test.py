import requests
import time

# --- CONFIGURATION ---
API_KEY = "37e0830c723c38868d630c9d251f23b3"
NEW_FORM_ID = "260192485390460"
ENDPOINT = f"https://api.jotform.com/form/{NEW_FORM_ID}/questions"

# --- FULL DATASET FROM TEMPLATE ---
# This includes all headers, inputs, hidden warnings, and upload fields.
questions_data = {
    "1": {"type": "control_head", "text": "Hello Support Network Application Form", "subHeader": "All information needed to track sales for Live Transfers Submit all the info to submit an application", "order": "1"},
    "29": {"type": "control_radio", "text": "Accident within last 12 Month ?", "options": "Yes|No", "required": "Yes", "order": "2"},
    "32": {"type": "control_text", "text": "<p><span style=\"background-color: #ea3223; color: #ffffff;\">We’re sorry, but we cannot take this transfer...</span></p>", "hidden": "Yes", "order": "3"},
    "13": {"type": "control_datetime", "text": "Date of Accident", "format": "mmddyyyy", "required": "Yes", "order": "4"},
    "33": {"type": "control_radio", "text": "Any Prior Attorney Involved ?", "options": "Yes|No", "required": "Yes", "order": "5"},
    "34": {"type": "control_text", "text": "<p><span style=\"background-color: #ea3223; color: #ffffff;\">We’re sorry, but we cannot take this transfer the customer must not have an attorney involved</span></p>", "hidden": "Yes", "order": "6"},
    "14": {"type": "control_textbox", "text": "Prior Attorney Involved (if any)", "required": "Yes", "order": "7"},
    "26": {"type": "control_radio", "text": "Did the Other Party Admit Fault at the Scene?", "options": "YES|NO", "required": "Yes", "order": "8"},
    "40": {"type": "control_text", "text": "<p><span style=\"background-color: #ea3223; color: #efefef;\">We’re sorry, but we cannot take this transfer the other party must admit the fault.</span></p>", "hidden": "Yes", "order": "9"},
    "35": {"type": "control_radio", "text": "Medical Attention Within 2 weeks ?", "options": "Yes|No", "required": "Yes", "order": "10"},
    "36": {"type": "control_text", "text": "<p><span style=\"background-color: #ea3223; color: #f3f3f3;\">We’re sorry, but we cannot take this transfer the customer does not have Medical Attention within the 2 Weeks</span></p>", "hidden": "Yes", "order": "11"},
    "15": {"type": "control_textarea", "text": "Medical Attention Received / Hospitals Attended:", "required": "Yes", "order": "12"},
    "16": {"type": "control_radio", "text": "Did Police Attend the Accident? (Yes/No)", "options": "YES|NO", "required": "Yes", "order": "13"},
    "37": {"type": "control_text", "text": "<p><span style=\"background-color: #ea3223; color: #f3f3f3;\">We’re sorry, but we cannot take this transfer the customer must have police report.</span></p>", "hidden": "Yes", "order": "14"},
    "38": {"type": "control_textbox", "text": "Police Report Reference Number", "required": "No", "order": "15"},
    "17": {"type": "control_address", "text": "Exact Location of the Accident:", "required": "Yes", "order": "16"},
    "19": {"type": "control_textarea", "text": "Accident Scenario / How the Incident Happened:", "required": "Yes", "order": "17"},
    "20": {"type": "control_radio", "text": "Is Customer Insured?", "options": "YES|NO", "required": "Yes", "order": "18"},
    "39": {"type": "control_text", "text": "<p><span style=\"background-color: #ea3223; color: #efefef;\">We’re sorry, but we cannot take this transfer the customer must be insured.</span></p>", "hidden": "Yes", "order": "19"},
    "22": {"type": "control_textarea", "text": "Customer Injuries / Areas Affected:", "required": "Yes", "order": "20"},
    "23": {"type": "control_textbox", "text": "Customer Vehicle Registration Number:", "required": "Yes", "order": "21"},
    "24": {"type": "control_textbox", "text": "Customer Insurance Company Name:", "required": "Yes", "order": "22"},
    "25": {"type": "control_textbox", "text": "Third Party Vehicle Registration Number:", "required": "Yes", "order": "23"},
    "27": {"type": "control_number", "text": "Number of Passengers in Your Vehicle:", "required": "No", "order": "24"},
    "3": {"type": "control_fullname", "text": "Customer Name", "required": "Yes", "order": "25"},
    "5": {"type": "control_phone", "text": "Customer Phone Number", "required": "Yes", "inputMask": "enable", "inputMaskValue": "(###) ###-####", "order": "26"},
    "28": {"type": "control_datetime", "text": "Date of Birth", "required": "Yes", "format": "mmddyyyy", "order": "27"},
    "4": {"type": "control_email", "text": "E-mail", "required": "Yes", "order": "28"},
    "10": {"type": "control_address", "text": "Customer Address", "required": "Yes", "order": "29"},
    "41": {"type": "control_fileupload", "text": "Medical Treatment Proof", "extensions": "pdf, doc, docx, xls, xlsx, csv, txt, rtf, html, zip, mp3, wma, mpg, flv, avi, jpg, jpeg, png, gif", "order": "30"},
    "42": {"type": "control_fileupload", "text": "Insurance Documents", "extensions": "pdf, doc, docx, xls, xlsx, csv, txt, rtf, html, zip, mp3, wma, mpg, flv, avi, jpg, jpeg, png, gif", "order": "31"},
    "43": {"type": "control_fileupload", "text": "Police Report", "extensions": "pdf, doc, docx, xls, xlsx, csv, txt, rtf, html, zip, mp3, wma, mpg, flv, avi, jpg, jpeg, png, gif", "order": "32"},
    "9": {"type": "control_textarea", "text": "Additional Comments", "required": "No", "order": "33"},
    "2": {"type": "control_button", "text": "Submit", "buttonStyle": "black_blue", "order": "34"}
}

def create_form_questions():
    headers = {"apiKey": API_KEY}
    
    for qid, details in questions_data.items():
        # Construct parameters in the format Jotform API expects: question[key]
        payload = {}
        for key, value in details.items():
            payload[f"question[{key}]"] = value
        
        print(f"Adding QID {qid}: {details.get('text')[:30]}...")
        
        response = requests.post(ENDPOINT, data=payload, headers=headers)
        
        if response.status_code == 200:
            print(f" - SUCCESS: Question added.")
        else:
            print(f" - FAILED: {response.status_code} - {response.text}")
        
        # Brief sleep to avoid hitting API rate limits too hard
        time.sleep(0.5)

if __name__ == "__main__":
    create_form_questions()
