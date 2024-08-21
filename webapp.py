from flask import Flask, request, render_template, send_file, url_for
import os
import pandas as pd
import numpy as np
from werkzeug.utils import secure_filename
from datetime import datetime

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['EXPORT_FOLDER'] = 'exports'
app.config['MAX_CONTENT_LENGTH'] = 150 * 1024 * 1024

# Ensure the folders exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['EXPORT_FOLDER'], exist_ok=True)

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/upload/<task>')
def upload(task):
    if task == 'task1':
        return render_template('operation_ea.html')
    elif task == 'task2':
        return render_template('ztp_builder_channel.html')
    elif task == 'task3':
        return render_template('ztp_nobroker.html')
    elif task == 'task4':
        return render_template('engg_marketing_ea.html')
    else:
        return "Invalid task.", 400

@app.route('/process/operation_ea', methods=['POST'])
def process_operation_ea():
    return process_files(task='operation_ea')

@app.route('/process/ztp_builder_channel', methods=['POST'])
def process_ztp_builder_channel():
    return process_files(task='ztp_builder_channel')

@app.route('/process/ztp_nobroker', methods=['POST'])
def process_ztp_nobroker():
    return process_files(task='ztp_nobroker')

@app.route('/process/engg_marketing_ea', methods=['POST'])
def process_engg_marketing_ea():
    return process_files(task='engg_marketing_ea')

def process_files(task):
    file1 = request.files.get('file1')
    file2 = request.files.get('file2')

    if not file1 or not file2:
        return "Both files are required.", 400

    filename1 = secure_filename(file1.filename) if file1.filename else 'default_file1.csv'
    filename2 = secure_filename(file2.filename) if file2.filename else 'default_file2.xlsx'

    filepath1 = os.path.join(app.config['UPLOAD_FOLDER'], filename1)
    filepath2 = os.path.join(app.config['UPLOAD_FOLDER'], filename2)
    
    file1.save(filepath1)
    file2.save(filepath2)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_filename = f"Employee_Data_{timestamp}.xlsx"
    export_filepath = os.path.join(app.config['EXPORT_FOLDER'], export_filename)

    try:
        if task == 'operation_ea':
            data = pd.read_csv(filepath1, header=0)
            ea_data = pd.read_excel(filepath2, sheet_name='EA Details Update', header=0)
            data.columns = data.columns.str.lower().str.replace(' ', '_')
            ea_data.columns = ea_data.columns.str.lower().str.replace(' ', '_')
            data.replace(' ', np.nan, inplace=True)
            ea_data.replace(' ', np.nan, inplace=True)
            columns_to_drop = ['gender', 'primary_mobile', 'company', 'designation_code', 'business_unit', 
                                'current_office_area', 'office_location_code', 'current_location_type', 
                                'reporting_manager_name', 'reporting_manager_employee_id', 'personal_mobile_number', 
                                'personal_email_id', 'base_office_area', 'date_of_resignation', 'date_of_exit', 
                                'division_code', 'employee_subtype', 'position_id', 'contribution_level', 
                                'business_unit_code', 'age', 'total_experience', 'office_location', 
                                'last_incumbent_lwd', 'current_incumbent_start_date', 'separation_approved_date', 
                                'effective_from', 'is_checkin_allowed', 'absent_reminder', 'check-in_reminder', 
                                'bank_proof', 'current_location', 'lwd_as_per_notice_period', 'pf_contribution', 
                                'pf_restricted', 'uan_number', 'gratuity_number', 'pf_applicable_from', 
                                'pf_registration_location', 'restrict_company_pf', 'existing_member_of_eps', 
                                'pf_number', 'lwf_designation', 'lwf_state', 'lwf_applicable', 
                                'esic_registration_name', 'esic_registration_code', 'esic_applicable_from', 
                                'esic_number', 'esic_applicable', 'permanent_state', 'permanent_city', 
                                'current_address', 'personal_mobile_number_access', 'office_mobile_number', 
                                'state_for_professional_tax', 'aadhaar_number', 'bank_ifsc_code', 'bank_account_number', 
                                'bank_name', 'emergency_contact_name', 'emergency_contact_number']
            darwin_required_data = data.drop(columns=columns_to_drop, errors='ignore')
            initiated_ea = darwin_required_data[darwin_required_data['employee_id'].isin(ea_data['employee_id'])]
            ea_not_initiated = darwin_required_data[~darwin_required_data['employee_id'].isin(ea_data['employee_id'])]
            exclude_department = '|'.join(['Engineering', 'Marketing', 'Hood'])
            ops_employee = ea_not_initiated[~ea_not_initiated['current_department'].str.contains(exclude_department, case=False, na=False)]
            ops_employee_active = ops_employee[ops_employee['employment_status'] == "Active"]
            pattern = '|'.join(['NBTSA', 'NBTSD', 'NBTSI', 'NBTSO'])
            ea_not_initiated_ops_active_nbts = ops_employee_active[~ops_employee_active['employee_id'].str.contains(pattern, case=False, na=False)]
            ea_not_initiated_ops_active_nbts.to_excel(export_filepath, index=False, engine='openpyxl')

        elif task == 'ztp_builder_channel':
            darwin_report = pd.read_csv(filepath1, header=0)
            ztp_tracker = pd.read_excel(filepath2, sheet_name='Details Update', header=0)
            darwin_report.columns = darwin_report.columns.str.lower().str.replace(' ', '_')
            darwin_report.replace(' ', np.nan, inplace=True)
            ztp_tracker.columns = ztp_tracker.columns.str.lower().str.replace(' ', '_')
            columns_to_drop = ['gender', 'primary_mobile', 'company', 'designation_code', 'business_unit', 
                                'current_office_area', 'office_location_code', 'current_location_type', 
                                'reporting_manager_name', 'reporting_manager_employee_id', 'personal_mobile_number', 
                                'personal_email_id', 'base_office_area', 'date_of_resignation', 'date_of_exit', 
                                'division_code', 'employee_subtype', 'position_id', 'contribution_level', 
                                'business_unit_code', 'age', 'total_experience', 'office_location', 
                                'last_incumbent_lwd', 'current_incumbent_start_date', 'separation_approved_date', 
                                'effective_from', 'is_checkin_allowed', 'absent_reminder', 'check-in_reminder', 
                                'bank_proof', 'current_location', 'lwd_as_per_notice_period', 'pf_contribution', 
                                'pf_restricted', 'uan_number', 'gratuity_number', 'pf_applicable_from', 
                                'pf_registration_location', 'restrict_company_pf', 'existing_member_of_eps', 
                                'pf_number', 'lwf_designation', 'lwf_state', 'lwf_applicable', 
                                'esic_registration_name', 'esic_registration_code', 'esic_applicable_from', 
                                'esic_number', 'esic_applicable', 'permanent_state', 'permanent_city', 
                                'current_address', 'personal_mobile_number_access', 'office_mobile_number', 
                                'state_for_professional_tax', 'aadhaar_number', 'bank_ifsc_code', 'bank_account_number', 
                                'bank_name', 'emergency_contact_name', 'emergency_contact_number']
            darwin_required_data = darwin_report.drop(columns=columns_to_drop, errors='ignore')
            darwin_required_data_builder_channel = darwin_required_data[darwin_required_data['current_department'] == 'Builder Channel']
            darwin_required_data_initiated = darwin_required_data_builder_channel[darwin_required_data_builder_channel['employee_id'].isin(ztp_tracker['employee_id'])]
            darwin_required_data_not_initiated = darwin_required_data_builder_channel[~darwin_required_data_builder_channel['employee_id'].isin(ztp_tracker['employee_id'])]
            pattern = '|'.join(['NBTSA', 'NBTSD', 'NBTSI', 'NBTSO'])
            darwin_required_data_not_initiated_nbts = darwin_required_data_not_initiated[~darwin_required_data_not_initiated['employee_id'].str.contains(pattern, case=False, na=False)]
            not_initiated_active = darwin_required_data_not_initiated_nbts[darwin_required_data_not_initiated_nbts['employment_status'] == 'Active']
            not_initiated_active.to_excel(export_filepath, index=False, engine='openpyxl')

        elif task == 'ztp_nobroker':
            # Add logic for ztp_nobroker here
            pass

        elif task == 'engg_marketing_ea':
            # Add logic for engg_marketing_ea here
            pass

        else:
            return "Invalid task.", 400

        download_url = url_for('download_file', filename=export_filename)
        return render_template('success.html', download_url=download_url)

    except Exception as e:
        return f"An error occurred while processing the files: {e}", 500

@app.route('/downloads/<filename>')
def download_file(filename):
    return send_file(os.path.join(app.config['EXPORT_FOLDER'], filename), as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
    print("Process Complited")
