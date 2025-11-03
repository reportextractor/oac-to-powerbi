import csv
import json
import os
import uuid

def create_filter_config(row, index):
    """Create a filter configuration dictionary from a CSV row"""
    # Generate a unique ID for the filter
    filter_id = str(uuid.uuid4().hex[:20])
    
    # Extract relevant fields from the CSV row
    column_ref = f"{row['TableName']}.{row['ColumnName'].replace(' ', '')}"
    dashboard_name = row.get('DashboardName', 'default')
    dashboard_tab = row.get('WorksheetName', 'default')
    
    # Calculate position based on index to prevent overlap
    x_pos = 1200 + (index % 2) * 350
    y_pos = 200 + (index // 2) * 200
    
    # Create the config dictionary structure with exact format
    config = {
        "name": filter_id,
        "layouts": [{
            "id": 0,
            "position": {
                "x": x_pos,
                "y": y_pos,
                "z": 4001,
                "width": 336.05,
                "height": 171.72,
                "tabOrder": 4001 + index
            }
        }],
        "singleVisual": {
            "visualType": "slicer",
            "projections": {"Values": [{"queryRef": column_ref, "active": True}]},
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": "t", "Entity": row['TableName'], "Type": 0}],
                "Select": [{
                    "Column": {
                        "Expression": {"SourceRef": {"Source": "t"}},
                        "Property": row['ColumnName']
                    },
                    "Name": column_ref,
                    "NativeReferenceName": row['ColumnName']
                }]
            },
            "drillFilterOtherVisuals": True,
            "objects": {
                "data": [{
                    "properties": {
                        "mode": {"expr": {"Literal": {"Value": "'Dropdown'"}}}
                    }
                }],
                "header": [{
                    "properties": {
                        "text": {"expr": {"Literal": {"Value": f"'{row['PromptName']}'"}}}
                    }
                }]
            }
        }
    }
    
    # Create the output dictionary with dashboard association
    return {
        "dashboard": dashboard_name,
        "tab": dashboard_tab,
        "config": json.dumps(config, separators=(',', ':'))
    }

def main():
    # Define file paths
    input_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'filters.csv')
    output_file = os.path.join(os.path.dirname(__file__), '..', 'output', 'globalFilter.json')
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    try:
        # Read the CSV file
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # Filter for global filters only
        global_filters = [row for row in rows if row.get('FilterType') == 'globalFilterPrompt']
        
        # Convert each filter to the required format with index for positioning
        output_data = [create_filter_config(filter_row, idx) for idx, filter_row in enumerate(global_filters)]
        
        # Group by dashboard and tab
        dashboard_filters = {}
        for item in output_data:
            key = f"{item['dashboard']}::{item['tab']}"
            if key not in dashboard_filters:
                dashboard_filters[key] = []
            dashboard_filters[key].append(item['config'])
        
        # Write to output file with the exact format and dashboard association
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('[')
            is_first = True
            for key, configs in dashboard_filters.items():
                dashboard_name, tab_name = key.split('::', 1)
                for config in configs:
                    if not is_first:
                        f.write(',')
                    f.write('\n    ')
                    # Include dashboard and tab in the output
                    output_obj = {
                        "dashboard": dashboard_name,
                        "tab": tab_name,
                        "config": config
                    }
                    f.write(json.dumps(output_obj, separators=(',', ':')))
                    is_first = False
            f.write('\n]')
            
        print(f"Successfully created {output_file} with {len(output_data)} global filters across {len(dashboard_filters)} dashboard tabs.")
        
    except FileNotFoundError:
        print(f"Error: Could not find input file at {input_file}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
