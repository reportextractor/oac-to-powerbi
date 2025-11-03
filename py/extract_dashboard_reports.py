import csv
import os
import sys
import logging
import argparse
import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple
import re
from urllib.parse import unquote

# Logger setup: logs to file and console
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
file_handler = logging.FileHandler('extract_dashboard_reports.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def read_csv_as_dict(csv_path: str) -> List[Dict]:
    """
    Read CSV file and return as list of dictionaries.
    """
    if not os.path.exists(csv_path):
        logger.warning(f"CSV file not found: {csv_path}")
        return []
    try:
        with open(csv_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            logger.info(f"Read {len(rows)} rows from {csv_path}")
            return rows
    except Exception as e:
        logger.error(f"Failed to read CSV file {csv_path}: {e}")
        return []

def write_csv(path: str, fieldnames: List[str], rows: List[Dict]) -> None:
    """
    Write CSV file with given fieldnames and rows.
    """
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"Wrote {len(rows)} rows to {path}")
    except Exception as e:
        logger.error(f"Failed to write CSV file {path}: {e}")

def _filter_erroneous_tablenames_rows(rows: List[Dict]) -> Tuple[List[Dict], int]:
    """
    Remove rows where the 'TableNames' field is invalid for Worksheets.csv output.

    Current invalid cases handled:
    1) TableNames is empty/blank.
    2) TableNames erroneously contains a SUM(...) - SUM(...) expression of the form:
         SUM (saw_<n> by saw_1, saw_2, saw_3, saw_0, saw_10, saw_11, saw_12)
         - SUM (saw_<m> by saw_1, saw_2, saw_3, saw_0, saw_10, saw_11, saw_12)

    Returns (filtered_rows, removed_count).
    """
    by_clause = r"saw_1\s*,\s*saw_2\s*,\s*saw_3\s*,\s*saw_0\s*,\s*saw_10\s*,\s*saw_11\s*,\s*saw_12"
    pattern = re.compile(
        rf"^\s*SUM\s*\(saw_\d+\s+by\s+{by_clause}\)\s*-\s*SUM\s*\(saw_\d+\s+by\s+{by_clause}\)\s*$",
        re.IGNORECASE,
    )

    kept: List[Dict] = []
    removed = 0
    for r in rows:
        tval = str(r.get('TableNames', '') or '').strip()
        # Case 1: Empty/blank TableNames
        if not tval:
            removed += 1
            continue
        # Case 2: Erroneous SUM(...)-SUM(...) expression mistakenly in TableNames
        if pattern.match(tval):
            removed += 1
            continue
        kept.append(r)
    return kept, removed

def _normalize_columnnames_biserver_variables(rows: List[Dict]) -> Tuple[List[Dict], int]:
    """
    Normalize ColumnNames that are populated with OBIEE variable syntax for current month name.

    If ColumnNames equals the expression:
        @{
          biServer.variables['Rvar_Curr_MonthName']
        }
    then replace it with just:
        Rvar_Curr_MonthName

    Returns (updated_rows, replacement_count).
    """
    # Match variations in whitespace and quoting around the variable name
    pattern = re.compile(r"^\s*@\{\s*biServer\.variables\[(['\"])Rvar_Curr_MonthName\1\]\s*\}\s*$",
                         re.IGNORECASE)
    replaced = 0
    for r in rows:
        col = str(r.get('ColumnNames', '') or '')
        if pattern.match(col):
            r['ColumnNames'] = 'Rvar_Curr_MonthName'
            replaced += 1
    return rows, replaced

def create_chart_type_data(reports_data: List[Dict], charts_data: List[Dict], views_data: List[Dict]) -> List[Dict]:
    """
    Create ChartType data from XML extraction.
    """
    chart_type_rows = []
    chart_type_map = {}
    for chart in charts_data:
        chart_type_map[chart.get('view_name', '')] = chart.get('display_type', '')
    logger.info("Created chart type map for quick lookup")
    for view in views_data:
        view_name = view.get('view_name', '')
        view_type = view.get('view_xsi_type', '')
        if view_type == 'dvtchart':
            chart_type = chart_type_map.get(view_name, 'unknown')
        elif view_type == 'tableView':
            chart_type = 'table'
        elif view_type == 'pivotTableView':
            chart_type = 'pivot'
        elif view_type == 'compoundView':
            chart_type = 'compound'
        elif view_type == 'titleView':
            chart_type = 'title'
        else:
            chart_type = view_type
        chart_type_rows.append({
            'WorkbookName': view.get('report_file', ''),
            'WorksheetName': view.get('report_file', ''),
            'ChartType': chart_type,
        })
    logger.info(f"Prepared chart type data with {len(chart_type_rows)} entries")
    return chart_type_rows

def create_filters_data(reports_data: List[Dict], columns_data: List[Dict],
                        column_orders_data: List[Dict], chart_categories_data: List[Dict],
                        chart_measures_data: List[Dict]) -> List[Dict]:
    """
    Create Filters data from XML extraction.
    """
    filter_rows = []
    column_map = {}
    for col in columns_data:
        column_map[col.get('column_id', '')] = col
    # Build column map for filters creation
    for order in column_orders_data:
        report_name = order.get('report_file', '')
        col_id = order.get('column_id', '')
        direction = order.get('direction', '')
        col_info = column_map.get(col_id, {})
        expression = col_info.get('expression', '')
        filter_rows.append({
            'WorkbookName': report_name,
            'SharedViewName': '',
            'WorksheetName': '',
            'Class': 'Sort',
            'Column': expression,
            'Function': direction,
            'ui-enumeration': '',
            'InstanceName': '',
            'ObjectName': col_id,
            'Member': '',
            'ui-domain': '',
            'ui-marker': '',
            'expression': expression,
            'ui-pattern_text': f'{direction} sort',
            'ui-pattern_type': 'sort',
        })
    logger.info(f"Added {len(column_orders_data)} sort filter rows")

    for category in chart_categories_data:
        report_name = category.get('report_file', '')
        view_name = category.get('view_name', '')
        col_id = category.get('column_id', '')
        col_info = column_map.get(col_id, {})
        expression = col_info.get('expression', '')
        filter_rows.append({
            'WorkbookName': report_name,
            'SharedViewName': '',
            'WorksheetName': view_name,
            'Class': 'Include',
            'Column': expression,
            'Function': 'Category',
            'ui-enumeration': '',
            'InstanceName': '',
            'ObjectName': col_id,
            'Member': '',
            'ui-domain': '',
            'ui-marker': '',
            'expression': expression,
            'ui-pattern_text': 'Chart category',
            'ui-pattern_type': 'include',
        })
    logger.info(f"Added {len(chart_categories_data)} category filter rows")

    for measure in chart_measures_data:
        report_name = measure.get('report_file', '')
        view_name = measure.get('view_name', '')
        col_id = measure.get('column_id', '')
        measure_type = measure.get('measure_type', '')
        riser_type = measure.get('riser_type', '')
        col_info = column_map.get(col_id, {})
        expression = col_info.get('expression', '')
        filter_rows.append({
            'WorkbookName': report_name,
            'SharedViewName': '',
            'WorksheetName': view_name,
            'Class': 'Include',
            'Column': expression,
            'Function': f'{measure_type} {riser_type}',
            'ui-enumeration': '',
            'InstanceName': '',
            'ObjectName': col_id,
            'Member': '',
            'ui-domain': '',
            'ui-marker': '',
            'expression': expression,
            'ui-pattern_text': f'Chart measure ({riser_type})',
            'ui-pattern_type': 'include',
        })
    logger.info(f"Added {len(chart_measures_data)} measure filter rows")
    logger.info(f"Total filter rows: {len(filter_rows)}")
    return filter_rows

def create_windows_data(reports_data: List[Dict], views_data: List[Dict], charts_data: List[Dict]) -> List[Dict]:
    """
    Create Windows data from XML extraction.
    """
    window_rows = []
    chart_map = {}
    for chart in charts_data:
        chart_map[chart.get('view_name', '')] = chart
    logger.info("Built chart map for window creation")
    for view in views_data:
        report_name = view.get('report_file', '')
        view_name = view.get('view_name', '')
        view_type = view.get('view_xsi_type', '')
        if view_type == 'dvtchart':
            window_class = 'Chart'
            zoom_type = 'entire-view'
            maximized = 'False'
        elif view_type == 'tableView':
            window_class = 'Table'
            zoom_type = 'fit-width'
            maximized = 'False'
        elif view_type == 'pivotTableView':
            window_class = 'Pivot'
            zoom_type = 'fit-width'
            maximized = 'False'
        elif view_type == 'compoundView':
            window_class = 'Compound'
            zoom_type = 'entire-view'
            maximized = 'False'
        elif view_type == 'titleView':
            window_class = 'Title'
            zoom_type = 'entire-view'
            maximized = 'False'
        else:
            window_class = 'View'
            zoom_type = 'entire-view'
            maximized = 'False'
        chart_info = chart_map.get(view_name, {})
        height = chart_info.get('canvas_height', '400') if window_class == 'Chart' else '400'
        width = chart_info.get('canvas_width', '800') if window_class == 'Chart' else '800'
        x_pos = '0'
        y_pos = '0'
        if view_type == 'titleView':
            y_pos = '0'
        elif view_type == 'dvtchart':
            y_pos = '100'
        elif view_type == 'tableView':
            y_pos = '200'
        elif view_type == 'pivotTableView':
            y_pos = '200'
        window_rows.append({
            'WorkbookName': report_name,
            'WindowName': view_name,
            'WindowClass': window_class,
            'Maximized': maximized,
            'Hidden': 'False',
            'ViewpointName': view_name,
            'ZoomType': zoom_type,
            'HighlightFields': 'False',
            'Max Height': height,
            'Max Width': width,
            'x_p': x_pos,
            'y_p': y_pos,
            'h_p': height,
            'w_p': width,
        })
    logger.info(f"Prepared window data with {len(window_rows)} entries")
    return window_rows

def create_worksheets_data(reports_data: List[Dict], columns_data: List[Dict], views_data: List[Dict],
                           edge_layers_data: List[Dict], chart_measures_data: List[Dict],
                           chart_categories_data: List[Dict], pivot_measures_data: List[Dict]) -> List[Dict]:
    """
    Create Worksheets data from XML extraction.
    """
    worksheet_rows = []
    column_map = {}
    for col in columns_data:
        column_map[col.get('column_id', '')] = col
    # Create column map for worksheets
    for report in reports_data:
        report_name = report.get('report_file', '')
        report_views = [v for v in views_data if v.get('report_file') == report_name]
        for view in report_views:
            view_name = view.get('view_name', '')
            view_type = view.get('view_xsi_type', '')
            view_edges = [e for e in edge_layers_data if e.get('report_file') == report_name and e.get('view_name') == view_name]
            view_chart_measures = [cm for cm in chart_measures_data if cm.get('report_file') == report_name and cm.get('view_name') == view_name]
            view_chart_categories = [cc for cc in chart_categories_data if cc.get('report_file') == report_name and cc.get('view_name') == view_name]
            view_pivot_measures = [pm for pm in pivot_measures_data if pm.get('report_file') == report_name and pm.get('view_name') == view_name]
            all_columns = set()
            for edge in view_edges:
                col_id = edge.get('column_id', '')
                if col_id and col_id in column_map:
                    all_columns.add(col_id)
            for measure in view_chart_measures:
                col_id = measure.get('column_id', '')
                if col_id and col_id in column_map:
                    all_columns.add(col_id)
            for category in view_chart_categories:
                col_id = category.get('column_id', '')
                if col_id and col_id in column_map:
                    all_columns.add(col_id)
            for pivot_measure in view_pivot_measures:
                col_id = pivot_measure.get('column_id', '')
                if col_id and col_id in column_map:
                    all_columns.add(col_id)
            for col_id in all_columns:
                col_info = column_map.get(col_id, {})
                expression = col_info.get('expression', '')
                object_type = 'Column'
                if col_id in [cm.get('column_id', '') for cm in view_chart_measures]:
                    object_type = 'Measure'
                elif col_id in [pm.get('column_id', '') for pm in view_pivot_measures]:
                    object_type = 'Measure'
                table_name = ''
                source_column = ''
                if expression and '.' in expression:
                    parts = expression.split('.')
                    if len(parts) >= 2:
                        table_name = parts[0].strip('"')
                        source_column = parts[1].strip('"')
                data_type = 'string'
                if 'Amount' in source_column or 'Quantity' in source_column:
                    data_type = 'number'
                elif 'year' in source_column.lower() or 'quarter' in source_column.lower():
                    data_type = 'date'
                derivation = 'None'
                if 'sqlExpression' in col_info.get('expr_xsi_type', ''):
                    derivation = 'Column'
                worksheet_rows.append({
                    'WorkbookName': report_name,
                    'WorksheetName': view_name,
                    'DataSourceName': report.get('subject_area', '').strip('"'),
                    'ObjectName_x': col_id,
                    'SourceColumn': source_column,
                    'ObjectType': object_type,
                    'DataType': data_type,
                    'Formula': expression,
                    'Format': '',
                    'SummarizeBy': '',
                    'InstanceName': f'[{col_id}]',
                    'Derivation': derivation,
                    'Pivot': 'True' if 'pivot' in view_type else 'False',
                    'Type': 'nominal' if object_type == 'Column' else 'quantitative',
                    'DataSource': report.get('subject_area', '').strip('"'),
                    'TableName': table_name,
                    'LocalName': source_column,
                    'DisplayName': source_column,
                    'X': '0',
                    'Y': '0',
                    'Slicer': 'False',
                    'Encoding': '',
                })
    logger.info(f"Prepared worksheets data with {len(worksheet_rows)} entries")
    return worksheet_rows

# def create_parameters_data(reports_data: List[Dict]) -> List[Dict]:
#     """
#     Create Parameters data from XML extraction.
#     """
#     parameter_rows = []
#     for report in reports_data:
#         report_name = report.get('report_file', '')
#         subject_area = report.get('subject_area', '').strip('"')
#         parameter_rows.append({
#             'WorkbookName': report_name,
#             'DataSourceName': 'Parameters',
#             'DataSource': 'Parameters',
#             'ObjectName': 'Subject Area',
#             'TableName': '',
#             'ObjectType': 'Parameter',
#             'LocalName': 'SubjectArea',
#             'SourceColumn': '',
#             'DataType': 'string',
#             'T_Expression': f'"{subject_area}"',
#             'Expression': f'"{subject_area}"',
#             'HideObject': '',
#             'Format': '',
#             'PrimaryKey': '',
#             'SummarizeBy': '',
#             'DisplayFolder': 'Parameters',
#             'DataCategory': '',
#             'SortByColumn': '',
#             'Description': '',
#             'DisplayName': 'Subject Area',
#             'TableName_D': '',
#             'P_Expression': f'"{subject_area}"',
#         })
#     logger.info(f"Prepared parameters data with {len(parameter_rows)} entries")
#     return parameter_rows

NAMESPACES: Dict[str, str] = {
    'saw': 'com.siebel.analytics.web/report/v1.1',
    'sawx': 'com.siebel.analytics.web/expression/v1.1',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'xsd': 'http://www.w3.org/2001/XMLSchema',
    'sawd': 'com.siebel.analytics.web/dashboard/v1.1',
}

def ensure_dir(path: str) -> None:
    """
    Ensure a directory exists; create it if missing.
    """
    try:
        if not os.path.isdir(path):
            os.makedirs(path, exist_ok=True)
            logger.info(f"Created directory: {path}")
        else:
            logger.debug(f"Directory already exists: {path}")
    except Exception as exc:
        logger.error(f"Failed to ensure directory {path}: {exc}")
        raise

def text(elem: ET.Element) -> str:
    """
    Extract trimmed text from an element, including all nested text; empty string if None.
    For multiline content, joins all text with spaces and normalizes whitespace.
    """
    if elem is None:
        return ''
    # Use itertext() to get all text content including from child elements
    all_text = ''.join(elem.itertext())
    # Normalize whitespace: replace multiple spaces/newlines with single space
    value = ' '.join(all_text.split())
    logger.debug(f"text() -> '{value[:100]}'" + ("..." if len(value) > 100 else ""))
    return value

def get_attr(elem: ET.Element, name: str, default: str = '') -> str:
    """
    Get attribute from element with default for None or missing.
    """
    if elem is None:
        logger.debug(f"get_attr(None, {name}) -> default '{default}'")
        return default
    value = str(elem.attrib.get(name, default))
    logger.debug(f"get_attr(..., {name}) -> '{value}'")
    return value

def find(elem: ET.Element, path: str) -> ET.Element:
    """
    Namespaced find.
    """
    found = elem.find(path, NAMESPACES) if elem is not None else None
    logger.debug(f"find(path='{path}') -> {'found' if found is not None else 'None'}")
    return found

def findall(elem: ET.Element, path: str) -> List[ET.Element]:
    """
    Namespaced findall.
    """
    results = elem.findall(path, NAMESPACES) if elem is not None else []
    logger.debug(f"findall(path='{path}') -> {len(results)} elements")
    return results

def strip_prefix(value: str, prefix: str) -> str:
    """
    Strip prefix from string if present.
    """
    if not value:
        logger.debug("strip_prefix: empty value")
        return value
    out = value[len(prefix):] if value.startswith(prefix) else value
    logger.debug(f"strip_prefix('{prefix}') -> '{out}' from '{value}'")
    return out

_VIEW_SUFFIX_RE = re.compile(r"!\d+$")

def normalize_view_name(name: str) -> str:
    """
    Normalize a view name by removing suffix like '!1'.
    """
    if not name:
        logger.debug("normalize_view_name: empty name")
        return name
    normalized = _VIEW_SUFFIX_RE.sub('', name)
    if normalized != name:
        logger.debug(f"normalize_view_name: '{name}' -> '{normalized}'")
    return normalized

def parse_report(xml_path: str) -> Tuple[
    List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict],
    List[Dict], List[Dict], List[Dict], List[Dict]
]:
    """
    Parse a single OBIEE report XML and extract multiple datasets.
    Returns 10 lists corresponding to:
    reports, columns, column_orders, views, edges, edge_layers, charts,
    chart_categories, chart_measures, pivot_measures_list
    """
    logger.info(f"Parsing XML: {xml_path}")
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except Exception as exc:
        logger.error(f"Failed to parse XML '{xml_path}': {exc}")
        raise

    report_rows: List[Dict] = []
    column_rows: List[Dict] = []
    column_order_rows: List[Dict] = []
    view_rows: List[Dict] = []
    edge_rows: List[Dict] = []
    edge_layer_rows: List[Dict] = []
    chart_rows: List[Dict] = []
    chart_category_rows: List[Dict] = []
    chart_measure_rows: List[Dict] = []
    measures_list_rows: List[Dict] = []

    # Normalize report file: strip .xml suffix
    report_name = os.path.splitext(os.path.basename(xml_path))[0]
    # Extract report base name

    # Report-level information
    criteria = find(root, 'saw:criteria')
    subject_area = get_attr(criteria, 'subjectArea') if criteria is not None else ''
    # Fallback: if top-level criteria lacks a subjectArea (e.g., derivedCriteria),
    # search nested simpleCriteria nodes under <saw:from> (or anywhere) for subjectArea.
    # When multiple subject areas are found, deduplicate and join with ' | '.
    if not subject_area:
        try:
            nested_sas = []
            for crit in findall(root, './/saw:criteria'):
                xsi_t = strip_prefix(get_attr(crit, '{%s}type' % NAMESPACES['xsi']), 'saw:')
                if xsi_t == 'simpleCriteria':
                    sa = get_attr(crit, 'subjectArea', '')
                    if sa:
                        # Normalize by stripping surrounding quotes for consistency
                        sa_clean = sa.strip('"')
                        if sa_clean not in nested_sas:
                            nested_sas.append(sa_clean)
            if nested_sas:
                # Store unquoted; downstream consumers already strip quotes if present
                subject_area = ' | '.join(nested_sas)
        except Exception as _:
            # Ignore fallback errors; leave subject_area as empty
            pass
    within_hierarchy = get_attr(criteria, 'withinHierarchy') if criteria is not None else ''
    xml_version = get_attr(root, 'xmlVersion')
    xsi_type = get_attr(criteria, '{%s}type' % NAMESPACES['xsi']) if criteria is not None else ''
    xsi_type = strip_prefix(xsi_type, 'saw:')

    report_rows.append({
        'report_file': report_name,
        'xml_version': xml_version,
        'criteria_type': xsi_type,
        'subject_area': subject_area,
        'within_hierarchy': within_hierarchy,
    })
    # Report info appended

    # Columns in criteria
    for col in findall(root, 'saw:criteria/saw:columns/saw:column'):
        col_id = get_attr(col, 'columnID')
        col_type = strip_prefix(get_attr(col, '{%s}type' % NAMESPACES['xsi']), 'saw:')
        expr_elem = find(col, 'saw:columnFormula/sawx:expr')
        expr_type = strip_prefix(strip_prefix(get_attr(expr_elem, '{%s}type' % NAMESPACES['xsi']), 'saw:'), 'sawx:') if expr_elem is not None else ''
        expr_text = text(expr_elem)
        
        # Extract table heading and column heading
        table_heading_elem = find(col, 'saw:tableHeading/saw:caption/saw:text')
        table_heading = text(table_heading_elem) if table_heading_elem is not None else ''
        
        column_heading_elem = find(col, 'saw:columnHeading/saw:caption/saw:text')
        column_heading = text(column_heading_elem) if column_heading_elem is not None else ''
        
        column_rows.append({
            'report_file': report_name,
            'column_id': col_id,
            'column_xsi_type': col_type,
            'expr_xsi_type': expr_type,
            'expression': expr_text,
            'table_heading': table_heading,
            'column_heading': column_heading,
        })
    logger.info(f"Columns extracted: {len(column_rows)} for '{report_name}'")

    # Column order (if any)
    for order_ref in findall(root, 'saw:criteria/saw:columnOrder/saw:columnOrderRef'):
        column_order_rows.append({
            'report_file': report_name,
            'column_id': get_attr(order_ref, 'columnID'),
            'direction': get_attr(order_ref, 'direction'),
        })
    logger.info(f"Column orders extracted: {len(column_order_rows)} for '{report_name}'")

    # Views
    views_parent = find(root, 'saw:views')
    current_view_name = get_attr(views_parent, 'currentView') if views_parent is not None else ''
    
    # Extract compound view children (preserve order)
    # Keep BOTH raw names (with !index) and normalized names
    compound_children_order = []  # normalized names
    compound_children_order_raw = []  # raw names with suffix like '!1'
    compound_view_children_raw_set = set()
    for compound_view in findall(root, 'saw:views/saw:view[@xsi:type="saw:compoundView"]'):
        # Extract view names from cvCell viewName attributes in order
        for cv_cell in findall(compound_view, './/saw:cvCell'):
            child_view_name_raw = get_attr(cv_cell, 'viewName')
            if child_view_name_raw:
                child_view_name_norm = normalize_view_name(child_view_name_raw)
                compound_children_order.append(child_view_name_norm)
                compound_children_order_raw.append(child_view_name_raw)
                compound_view_children_raw_set.add(child_view_name_raw)
    
    for view in findall(root, 'saw:views/saw:view'):
        view_name_raw = get_attr(view, 'name')
        view_name = normalize_view_name(view_name_raw)
        view_type = strip_prefix(get_attr(view, '{%s}type' % NAMESPACES['xsi']), 'saw:')
        view_rows.append({
            'report_file': report_name,
            'view_name': view_name,
            'view_name_raw': view_name_raw,
            'view_xsi_type': view_type,
            'current_view': current_view_name,
            # Mark membership using RAW names to avoid collapsing distinct views like pivotTableView!1 vs !2
            'in_compound_view': view_name_raw in compound_view_children_raw_set or view_type == 'compoundView',
            'compound_children_order': compound_children_order if view_type == 'compoundView' else [],
            'compound_children_order_raw': compound_children_order_raw if view_type == 'compoundView' else [],
        })
    logger.info(f"Views extracted: {len(view_rows)} for '{report_name}' (compound view children: {len(compound_children_order)})")

    # Table/pivot edges and layers
    table_like_views = findall(root, 'saw:views/saw:view[@xsi:type="saw:tableView"]') + \
                       findall(root, 'saw:views/saw:view[@xsi:type="saw:pivotTableView"]')
    for table_like in table_like_views:
        view_name = normalize_view_name(get_attr(table_like, 'name'))
        for edge in findall(table_like, 'saw:edges/saw:edge'):
            axis = get_attr(edge, 'axis')
            show_header = get_attr(edge, 'showColumnHeader')
            edge_rows.append({
                'report_file': report_name,
                'view_name': view_name,
                'axis': axis,
                'show_column_header': show_header,
            })
            for layer in findall(edge, 'saw:edgeLayers/saw:edgeLayer'):
                edge_layer_rows.append({
                    'report_file': report_name,
                    'view_name': view_name,
                    'axis': axis,
                    'layer_type': get_attr(layer, 'type'),
                    'column_id': get_attr(layer, 'columnID'),
                    'agg_rule': get_attr(layer, 'aggRule'),
                })
    logger.info(f"Edges extracted: {len(edge_rows)}; Edge layers: {len(edge_layer_rows)} for '{report_name}'")

    # Pivot measures list
    for pivot in findall(root, 'saw:views/saw:view[@xsi:type="saw:pivotTableView"]'):
        view_name = normalize_view_name(get_attr(pivot, 'name'))
        for measure in findall(pivot, 'saw:measuresList/saw:measure'):
            measures_list_rows.append({
                'report_file': report_name,
                'view_name': view_name,
                'column_id': get_attr(measure, 'columnID'),
                'agg_rule': get_attr(measure, 'aggRule'),
            })
    logger.info(f"Pivot measures list extracted: {len(measures_list_rows)} for '{report_name}'")

    # Charts and selections
    for chart in findall(root, 'saw:views/saw:view[@xsi:type="saw:dvtchart"]'):
        view_name = normalize_view_name(get_attr(chart, 'name'))
        display = find(chart, 'saw:display')
        style = find(chart, 'saw:display/saw:style')
        canvas = find(chart, 'saw:canvasFormat')
        data_labels = find(chart, 'saw:canvasFormat/saw:dataLabels')
        legend = find(chart, 'saw:legendFormat')

        chart_rows.append({
            'report_file': report_name,
            'view_name': view_name,
            'display_type': get_attr(display, 'type'),
            'display_subtype': get_attr(display, 'subtype'),
            'render_format': get_attr(display, 'renderFormat'),
            'display_mode': get_attr(display, 'mode'),
            'bar_style': get_attr(style, 'barStyle'),
            'line_style': get_attr(style, 'lineStyle'),
            'scatter_style': get_attr(style, 'scatterStyle'),
            'fill_style': get_attr(style, 'fillStyle'),
            'bubble_percent_size': get_attr(style, 'bubblePercentSize'),
            'effect': get_attr(style, 'effect'),
            'canvas_height': get_attr(canvas, 'height'),
            'canvas_width': get_attr(canvas, 'width'),
            'data_labels_display': get_attr(data_labels, 'display'),
            'data_labels_label': get_attr(data_labels, 'label'),
            'data_labels_position': get_attr(data_labels, 'position'),
            'data_labels_transparent': get_attr(data_labels, 'transparentBackground'),
            'data_labels_value_as': get_attr(data_labels, 'valueAs'),
            'legend_position': get_attr(legend, 'position'),
            'legend_transparent_fill': get_attr(legend, 'transparentFill'),
        })

        # Categories
        # Typical bar/line charts use categories/category/columnRef; pie charts often encode the
        # slice dimension under seriesGenerators/seriesGenerator/columnRef while categories contains
        # only <measureLabels/>. Capture both forms as categories for downstream processing.
        for category in findall(chart, 'saw:selections/saw:categories/saw:category'):
            col_ref = find(category, 'saw:columnRef')
            chart_category_rows.append({
                'report_file': report_name,
                'view_name': view_name,
                'column_id': get_attr(col_ref, 'columnID'),
            })
        # Also treat seriesGenerators with columnRef as category dimensions (e.g., pie charts)
        for series_gen in findall(chart, 'saw:selections/saw:seriesGenerators/saw:seriesGenerator'):
            col_ref = find(series_gen, 'saw:columnRef')
            if col_ref is not None:
                chart_category_rows.append({
                    'report_file': report_name,
                    'view_name': view_name,
                    'column_id': get_attr(col_ref, 'columnID'),
                })

        # Measures
        for meas in findall(chart, 'saw:selections/saw:measures/saw:column'):
            col_ref = find(meas, 'saw:columnRef')
            chart_measure_rows.append({
                'report_file': report_name,
                'view_name': view_name,
                'measure_type': get_attr(meas, 'measureType'),
                'riser_type': get_attr(meas, 'riserType'),
                'column_id': get_attr(col_ref, 'columnID'),
            })

    logger.info(f"Charts extracted: {len(chart_rows)}; categories: {len(chart_category_rows)}; measures: {len(chart_measure_rows)} for '{report_name}'")

    return (
        report_rows,
        column_rows,
        column_order_rows,
        view_rows,
        edge_rows,
        edge_layer_rows,
        chart_rows,
        chart_category_rows,
        chart_measure_rows,
        measures_list_rows,
    )

def parse_dashboard_layout(xml_path: str, dashboard_name: str = '') -> Tuple[Dict, List[Dict]]:
    """
    Parse a dashboard layout XML file and extract dashboard info and page references.
    Returns: (dashboard_info_dict, list_of_page_refs)
    """
    logger.info(f"Parsing dashboard layout XML: {xml_path}")
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except Exception as exc:
        logger.error(f"Failed to parse dashboard layout XML '{xml_path}': {exc}")
        raise
    
    # Extract dashboard-level attributes
    dashboard_info = {
        'dashboard_name': dashboard_name,
        'dashboard_file': xml_path,
        'style': get_attr(root, 'style'),
        'fit_content': get_attr(root, 'fitContent'),
        'xml_version': get_attr(root, 'xmlVersion'),
        'get_tab_with_action_link': get_attr(root, 'getTabWithActionLink'),
        'prompts_auto_complete': get_attr(root, 'promptsAutoComplete'),
    }
    
    # Extract dashboard page references
    page_refs = []
    for page_ref in findall(root, 'sawd:dashboardPageRef'):
        page_refs.append({
            'dashboard_name': dashboard_name,
            'page_path': get_attr(page_ref, 'path'),
            'page_type': get_attr(page_ref, 'type'),
            'hidden': get_attr(page_ref, 'hidden'),
        })
    
    logger.info(f"Dashboard layout parsed: {len(page_refs)} page references found")
    return dashboard_info, page_refs

def parse_dashboard_page(xml_path: str, page_name: str = '', dashboard_name: str = '', dashboard_duid: str = '') -> Tuple[
    Dict, List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]
]:
    """
    Parse a dashboard page XML file and extract page info, columns, sections, report views, filters, and action links.
    Returns: (page_info, columns, sections, report_views, global_filters, action_links)
    """
    logger.info(f"Parsing dashboard page XML: {xml_path}")
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except Exception as exc:
        logger.error(f"Failed to parse dashboard page XML '{xml_path}': {exc}")
        raise
    
    # Extract page-level attributes
    page_duid = get_attr(root, 'duid')
    page_info = {
        'dashboard_name': dashboard_name,
        'page_name': page_name,
        'page_file': xml_path,
        'xml_version': get_attr(root, 'xmlVersion'),
        'is_empty': get_attr(root, 'isEmpty'),
        'duid': page_duid,
        'parent_duid': dashboard_duid,
    }
    
    columns_list = []
    sections_list = []
    report_views_list = []
    global_filters_list = []
    action_links_list = []
    
    # Extract dashboard columns
    for col_idx, column in enumerate(findall(root, 'sawd:dashboardColumn')):
        column_name = get_attr(column, 'name')
        column_duid = get_attr(column, 'duid')
        column_info = {
            'dashboard_name': dashboard_name,
            'page_name': page_name,
            'column_name': column_name,
            'column_index': col_idx,
            'frozen': get_attr(column, 'frozen'),
            'can_freeze': get_attr(column, 'canFreeze'),
            'layout_type': get_attr(column, 'layoutType'),
            'duid': column_duid,
            'parent_duid': page_duid,
        }
        columns_list.append(column_info)
        
        # Extract sections within this column
        for sec_idx, section in enumerate(findall(column, 'sawd:dashboardSection')):
            section_name = get_attr(section, 'name')
            section_duid = get_attr(section, 'duid')
            section_info = {
                'dashboard_name': dashboard_name,
                'page_name': page_name,
                'column_name': column_name,
                'section_name': section_name,
                'section_index': sec_idx,
                'layout_type': get_attr(section, 'layoutType'),
                'duid': section_duid,
                'parent_duid': column_duid,
                'show_section_title': get_attr(section, 'showSectionTitle'),
                'collapsible': get_attr(section, 'collapsible'),
                'horizontal_layout': get_attr(section, 'horizontalLayout'),
            }
            sections_list.append(section_info)
            
            # Extract report views within this section
            for rv_idx, report_view in enumerate(findall(section, 'sawd:reportView')):
                caption_elem = find(report_view, 'saw:caption/saw:text')
                caption_text = text(caption_elem) if caption_elem is not None else ''
                
                report_ref = find(report_view, 'sawd:reportRef')
                report_path = get_attr(report_ref, 'path') if report_ref is not None else ''
                # Unescape forward slashes in report path (XML uses \/ for /)
                report_path = report_path.replace('\\/', '/')
                report_type = get_attr(report_ref, 'type') if report_ref is not None else ''
                
                report_view_info = {
                    'dashboard_name': dashboard_name,
                    'page_name': page_name,
                    'page_file': xml_path,
                    'column_name': column_name,
                    'section_name': section_name,
                    'report_view_name': get_attr(report_view, 'name'),
                    'report_view_index': rv_idx,
                    'display': get_attr(report_view, 'display'),
                    'show_view': get_attr(report_view, 'showView'),
                    'duid': get_attr(report_view, 'duid'),
                    'parent_duid': section_duid,
                    'caption': caption_text,
                    'report_path': report_path,
                    'report_type': report_type,
                }
                report_views_list.append(report_view_info)
            
            # Extract global filter views within this section
            for gf_idx, global_filter in enumerate(findall(section, 'sawd:globalFilterView')):
                caption_elem = find(global_filter, 'saw:caption/saw:text')
                caption_text = text(caption_elem) if caption_elem is not None else ''
                
                global_filter_info = {
                    'dashboard_name': dashboard_name,
                    'page_name': page_name,
                    'page_file': xml_path,
                    'column_name': column_name,
                    'section_name': section_name,
                    'filter_name': get_attr(global_filter, 'name'),
                    'filter_index': gf_idx,
                    'filter_path': get_attr(global_filter, 'path'),
                    'duid': get_attr(global_filter, 'duid'),
                    'parent_duid': section_duid,
                    'caption': caption_text,
                }
                global_filters_list.append(global_filter_info)
            
            # Extract action link views within this section
            for al_idx, action_link_view in enumerate(findall(section, 'sawd:actionLinkView')):
                action_link = find(action_link_view, 'sawd:actionLink')
                if action_link is not None:
                    caption_elem = find(action_link, 'saw:caption/saw:text')
                    caption_text = text(caption_elem) if caption_elem is not None else ''
                    
                    # Try to extract navigation path from assignments
                    nav_path = ''
                    assign_elem = find(action_link, 'saw:action/saw:assignments/saw:assign/saw:value')
                    if assign_elem is not None:
                        nav_path = text(assign_elem)
                    
                    action_link_info = {
                        'dashboard_name': dashboard_name,
                        'page_name': page_name,
                        'page_file': xml_path,
                        'column_name': column_name,
                        'section_name': section_name,
                        'action_link_name': get_attr(action_link_view, 'name'),
                        'action_link_index': al_idx,
                        'duid': get_attr(action_link_view, 'duid'),
                        'parent_duid': section_duid,
                        'brief_book_link': get_attr(action_link, 'briefBookLink'),
                        'target': get_attr(action_link, 'target'),
                        'display_name': get_attr(action_link, 'sDisplayName'),
                        'caption': caption_text,
                        'navigation_path': nav_path,
                    }
                    action_links_list.append(action_link_info)
    
    logger.info(f"Dashboard page parsed: {len(columns_list)} columns, {len(sections_list)} sections, "
                f"{len(report_views_list)} report views, {len(global_filters_list)} global filters, "
                f"{len(action_links_list)} action links")
    
    return page_info, columns_list, sections_list, report_views_list, global_filters_list, action_links_list

def process_dashboard_directory(dashboard_dir: str, dashboard_name: str = '') -> Tuple[
    List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]
]:
    """
    Process a dashboard directory containing dashboard+layout.xml and page XML files.
    Returns: (dashboards, page_refs, pages, columns, sections, report_views, global_filters, action_links)
    """
    logger.info(f"Processing dashboard directory: {dashboard_dir}")
    
    dashboards_list = []
    page_refs_list = []
    pages_list = []
    columns_list = []
    sections_list = []
    report_views_list = []
    global_filters_list = []
    action_links_list = []
    
    # If no dashboard name provided, use directory name
    if not dashboard_name:
        dashboard_name = os.path.basename(dashboard_dir)
    
    dashboard_duid = ''
    
    # Parse dashboard layout file (try with and without .xml extension)
    layout_file = os.path.join(dashboard_dir, 'dashboard+layout.xml')
    if not os.path.exists(layout_file):
        layout_file = os.path.join(dashboard_dir, 'dashboard+layout')
    
    if os.path.exists(layout_file):
        try:
            dashboard_info, page_refs = parse_dashboard_layout(layout_file, dashboard_name)
            dashboard_duid = dashboard_info.get('xml_version', '')  # Use xml_version as fallback if no duid
            dashboards_list.append(dashboard_info)
            page_refs_list.extend(page_refs)
            
            # Parse each referenced page
            for page_ref in page_refs:
                page_path = page_ref['page_path']
                # Convert page path to filename (e.g., "Table views" -> "table+views" or "page+1")
                page_filename = page_path.lower().replace(' ', '+')
                
                # Try with .xml extension first, then without
                page_file = os.path.join(dashboard_dir, page_filename + '.xml')
                if not os.path.exists(page_file):
                    page_file = os.path.join(dashboard_dir, page_filename)
                
                if os.path.exists(page_file):
                    try:
                        page_info, columns, sections, report_views, global_filters, action_links = \
                            parse_dashboard_page(page_file, page_path, dashboard_name, dashboard_duid)
                        
                        pages_list.append(page_info)
                        columns_list.extend(columns)
                        sections_list.extend(sections)
                        report_views_list.extend(report_views)
                        global_filters_list.extend(global_filters)
                        action_links_list.extend(action_links)
                    except Exception as exc:
                        logger.warning(f"Failed to parse dashboard page '{page_file}': {exc}")
                else:
                    logger.warning(f"Dashboard page file not found: {page_file}")
        except Exception as exc:
            logger.warning(f"Failed to parse dashboard layout '{layout_file}': {exc}")
    else:
        logger.warning(f"Dashboard layout file not found: {layout_file}")
    
    # Also check for standalone page files (not referenced in layout)
    if os.path.isdir(dashboard_dir):
        for fname in os.listdir(dashboard_dir):
            if fname.lower().endswith('.xml') and fname != 'dashboard+layout.xml':
                page_file = os.path.join(dashboard_dir, fname)
                page_name = os.path.splitext(fname)[0].replace('+', ' ')
                
                # Check if this page was already processed
                if not any(p['page_name'] == page_name for p in pages_list):
                    try:
                        page_info, columns, sections, report_views, global_filters, action_links = \
                            parse_dashboard_page(page_file, page_name, dashboard_name, dashboard_duid)
                        
                        pages_list.append(page_info)
                        columns_list.extend(columns)
                        sections_list.extend(sections)
                        report_views_list.extend(report_views)
                        global_filters_list.extend(global_filters)
                        action_links_list.extend(action_links)
                    except Exception as exc:
                        logger.warning(f"Failed to parse standalone dashboard page '{page_file}': {exc}")
    
    logger.info(f"Dashboard directory processed: {len(dashboards_list)} dashboards, {len(pages_list)} pages")
    return dashboards_list, page_refs_list, pages_list, columns_list, sections_list, report_views_list, global_filters_list, action_links_list

def process_all_dashboards_recursively(root_dir: str) -> Tuple[
    List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]
]:
    """
    Recursively process all dashboard directories and XML files in root_dir.
    Returns: (dashboards, page_refs, pages, columns, sections, report_views, global_filters, action_links)
    """
    logger.info(f"Recursively scanning for dashboards in: {root_dir}")
    
    agg_dashboards = []
    agg_page_refs = []
    agg_pages = []
    agg_columns = []
    agg_sections = []
    agg_report_views = []
    agg_global_filters = []
    agg_action_links = []
    
    if not os.path.isdir(root_dir):
        logger.warning(f"Directory not found: {root_dir}")
        return agg_dashboards, agg_page_refs, agg_pages, agg_columns, agg_sections, agg_report_views, agg_global_filters, agg_action_links
    
    # Walk through all directories recursively
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Check if this directory contains a dashboard+layout file (with or without .xml extension)
        has_dashboard = False
        if 'dashboard+layout.xml' in filenames or 'dashboard+layout' in filenames:
            has_dashboard = True
        
        if has_dashboard:
            # Get relative path from root for dashboard name
            rel_path = os.path.relpath(dirpath, root_dir)
            dashboard_name = rel_path.replace(os.sep, '/')
            if dashboard_name == '.':
                dashboard_name = 'root'
            
            try:
                logger.info(f"Found dashboard in: {dirpath}")
                dashboards, page_refs, pages, columns, sections, report_views, global_filters, action_links = \
                    process_dashboard_directory(dirpath, dashboard_name)
                
                agg_dashboards.extend(dashboards)
                agg_page_refs.extend(page_refs)
                agg_pages.extend(pages)
                agg_columns.extend(columns)
                agg_sections.extend(sections)
                agg_report_views.extend(report_views)
                agg_global_filters.extend(global_filters)
                agg_action_links.extend(action_links)
            except Exception as exc:
                logger.warning(f"Failed to process dashboard directory '{dirpath}': {exc}")
    
    logger.info(f"Recursive scan complete: {len(agg_dashboards)} dashboards found")
    return agg_dashboards, agg_page_refs, agg_pages, agg_columns, agg_sections, agg_report_views, agg_global_filters, agg_action_links

def build_filter_expression_string(expr_elem) -> str:
    """
    Recursively build a consolidated filter expression string from XML.
    Returns a human-readable filter expression with logical operators.
    """
    if expr_elem is None:
        return ''
    
    op = get_attr(expr_elem, 'op', '')
    expr_type = strip_prefix(get_attr(expr_elem, '{%s}type' % NAMESPACES['xsi']), 'sawx:')
    
    # Handle logical operators (and, or) - recursively process children
    if expr_type == 'logical' and op in ('and', 'or'):
        child_exprs = findall(expr_elem, 'sawx:expr')
        child_strings = []
        for child in child_exprs:
            child_str = build_filter_expression_string(child)
            if child_str:
                child_strings.append(f"({child_str})")
        
        if child_strings:
            return f" {op.upper()} ".join(child_strings)
        return ''
    
    # Handle list operators (in) - must check before comparison
    elif expr_type == 'list' and op == 'in':
        child_exprs = findall(expr_elem, 'sawx:expr')
        if len(child_exprs) >= 1:
            column = text(child_exprs[0])
            values = [text(child) for child in child_exprs[1:]]
            if values:
                values_str = ', '.join(values)
                return f"{column} IN ({values_str})"
            else:
                return f"{column} IN ()"
        return ''
    
    # Handle comparison operators
    elif expr_type == 'comparison' or op in ('equal', 'in', 'greaterOrEqual', 'lessOrEqual', 'notEqual', 'greater', 'less'):
        child_exprs = findall(expr_elem, 'sawx:expr')
        if len(child_exprs) >= 2:
            # Extract left side - handle columnExpression type
            left_type = strip_prefix(get_attr(child_exprs[0], '{%s}type' % NAMESPACES['xsi']), 'sawx:')
            if left_type == 'columnExpression':
                # Extract from nested columnFormula
                formula_elem = find(child_exprs[0], 'saw:columnFormula/sawx:expr')
                if formula_elem is not None:
                    left = text(formula_elem)
                else:
                    left = text(child_exprs[0])
            else:
                left = text(child_exprs[0])
            
            right = text(child_exprs[1])
            op_symbol = {'equal': '=', 'notEqual': '!=', 'greaterOrEqual': '>=', 'lessOrEqual': '<=', 'greater': '>', 'less': '<'}.get(op, op)
            return f"{left} {op_symbol} {right}"
        elif len(child_exprs) == 1:
            # Single child expression (shouldn't happen but handle it)
            return text(child_exprs[0])
        return ''
    
    # Handle special operators like prompted
    elif expr_type == 'special' and op == 'prompted':
        child_exprs = findall(expr_elem, 'sawx:expr')
        if len(child_exprs) >= 1:
            # For prompted filters, only extract the sqlExpression, not all text
            child_type = strip_prefix(get_attr(child_exprs[0], '{%s}type' % NAMESPACES['xsi']), 'sawx:')
            if child_type == 'sqlExpression':
                column = text(child_exprs[0])
            elif child_type == 'columnExpression':
                # Extract from nested columnFormula
                formula_elem = find(child_exprs[0], 'saw:columnFormula/sawx:expr')
                if formula_elem is not None:
                    column = text(formula_elem)
                else:
                    column = child_exprs[0].text or ''
            else:
                # Fallback: get only direct text, not nested text
                column = child_exprs[0].text or ''
                if not column:
                    # Try to find first sqlExpression child
                    sql_expr = find(child_exprs[0], './/sawx:expr[@xsi:type="sawx:sqlExpression"]')
                    if sql_expr is not None:
                        column = text(sql_expr)
            
            if column:
                return f"{column} IS PROMPTED"
        return ''
    
    return ''

def parse_filter_expression(expr_elem, parent_op='') -> List[Dict]:
    """
    Parse filter expressions and return individual filter conditions as separate rows.
    Returns a list of dictionaries, one for each individual filter condition.
    """
    if expr_elem is None:
        return []
    
    op = get_attr(expr_elem, 'op', '')
    expr_type = strip_prefix(get_attr(expr_elem, '{%s}type' % NAMESPACES['xsi']), 'sawx:')
    
    # Handle logical operators (and, or) - recursively process children and return separate rows
    if expr_type == 'logical' and op in ('and', 'or'):
        child_exprs = findall(expr_elem, 'sawx:expr')
        all_filters = []
        for child in child_exprs:
            child_filters = parse_filter_expression(child, parent_op=op)
            all_filters.extend(child_filters)
        return all_filters
    
    # For non-logical expressions, build the expression string and extract column/table
    expression_string = build_single_filter_expression(expr_elem)
    
    if not expression_string:
        return []
    
    # Extract column and table name from this specific expression
    column_name = ''
    table_name = ''
    filter_value = ''
    
    # Extract from sqlExpression elements
    for sql_expr in findall(expr_elem, './/sawx:expr[@xsi:type="sawx:sqlExpression"]'):
        expr_text = text(sql_expr)
        if expr_text and '"' in expr_text:
            parts = expr_text.split('"')
            if len(parts) >= 2 and not table_name:
                table_name = parts[1]
            if len(parts) >= 4 and not column_name:
                column_name = parts[3]
    
    # Extract filter values based on operator type
    if op == 'prompted':
        # IS PROMPTED filters should have empty filter value
        filter_value = ''
    elif op == 'in':
        # IN operator: extract all values from the list
        child_exprs = findall(expr_elem, 'sawx:expr')
        values = []
        # Skip first child (it's the column), collect the rest as values
        for i, child in enumerate(child_exprs):
            if i == 0:
                continue  # Skip column expression
            val = text(child)
            if val:
                values.append(val)
        filter_value = ' | '.join(values) if values else ''
    elif op in ('equal', 'notEqual', 'greaterOrEqual', 'lessOrEqual', 'greater', 'less'):
        # Comparison operators: extract the right-hand value (second child)
        child_exprs = findall(expr_elem, 'sawx:expr')
        if len(child_exprs) >= 2:
            filter_value = text(child_exprs[1])
        else:
            filter_value = ''
    elif op == 'between':
        # BETWEEN operator: extract lower and upper bounds
        child_exprs = findall(expr_elem, 'sawx:expr')
        if len(child_exprs) >= 3:
            # Format: column BETWEEN lower AND upper
            lower = text(child_exprs[1])
            upper = text(child_exprs[2])
            filter_value = f"{lower} | {upper}"
        else:
            filter_value = ''
    elif op in ('null', 'notNull', 'isNull', 'isNotNull'):
        # NULL checks don't have values
        filter_value = ''
    else:
        # For other operators, try to extract any literal values
        for literal_expr in findall(expr_elem, './/sawx:expr[@xsi:type="xsd:string"]'):
            val = text(literal_expr)
            if val:
                filter_value = val
                break
    
    # If expression is incomplete or missing, reconstruct it
    if table_name and column_name:
        # Fix IS PROMPTED without column
        if expression_string and 'IS PROMPTED' in expression_string and not expression_string.startswith('"'):
            expression_string = f'"{table_name}"."{column_name}" IS PROMPTED'
        # Fix IN operator - if operator is 'in' but expression doesn't contain 'IN', reconstruct
        elif op == 'in' and (not expression_string or 'IN' not in expression_string.upper()):
            if filter_value:
                expression_string = f'"{table_name}"."{column_name}" IN ({filter_value.replace(" | ", ", ")})'
            else:
                expression_string = f'"{table_name}"."{column_name}" IN ()'
    
    return [{
        'operator': op,
        'parent_operator': parent_op,
        'column_expression': expression_string,
        'column_name': column_name,
        'table_name': table_name,
        'filter_value': filter_value,
    }]

def build_single_filter_expression(expr_elem) -> str:
    """
    Build a single filter expression string (non-recursive for logical operators).
    """
    if expr_elem is None:
        return ''
    
    op = get_attr(expr_elem, 'op', '')
    expr_type = strip_prefix(get_attr(expr_elem, '{%s}type' % NAMESPACES['xsi']), 'sawx:')
    
    # Don't recurse into logical operators - just build this single expression
    if expr_type == 'logical' and op in ('and', 'or'):
        return ''
    
    # Handle IN operators first - check operator before type
    if op == 'in':
        child_exprs = findall(expr_elem, 'sawx:expr')
        if len(child_exprs) >= 1:
            column = text(child_exprs[0])
            values = [text(child) for child in child_exprs[1:]]
            if values:
                values_str = ', '.join(values)
                return f"{column} IN ({values_str})"
            else:
                # Empty IN clause - still return the structure
                return f"{column} IN ()"
        return ''
    
    # Handle BETWEEN operator
    elif op == 'between':
        child_exprs = findall(expr_elem, 'sawx:expr')
        if len(child_exprs) >= 3:
            column = text(child_exprs[0])
            lower = text(child_exprs[1])
            upper = text(child_exprs[2])
            return f"{column} BETWEEN {lower} AND {upper}"
        elif len(child_exprs) >= 1:
            # Incomplete BETWEEN - just return column
            return text(child_exprs[0])
        return ''
    
    # Handle comparison operators
    elif expr_type == 'comparison' or op in ('equal', 'greaterOrEqual', 'lessOrEqual', 'notEqual', 'greater', 'less'):
        child_exprs = findall(expr_elem, 'sawx:expr')
        if len(child_exprs) >= 2:
            # Extract left side
            left_type = strip_prefix(get_attr(child_exprs[0], '{%s}type' % NAMESPACES['xsi']), 'sawx:')
            if left_type == 'columnExpression':
                formula_elem = find(child_exprs[0], 'saw:columnFormula/sawx:expr')
                if formula_elem is not None:
                    left = text(formula_elem)
                else:
                    left = text(child_exprs[0])
            else:
                left = text(child_exprs[0])
            
            right = text(child_exprs[1])
            op_symbol = {'equal': '=', 'notEqual': '!=', 'greaterOrEqual': '>=', 'lessOrEqual': '<=', 'greater': '>', 'less': '<'}.get(op, op)
            return f"{left} {op_symbol} {right}"
        elif len(child_exprs) == 1:
            return text(child_exprs[0])
        return ''
    
    # Handle special operators like prompted
    elif expr_type == 'special' and op == 'prompted':
        child_exprs = findall(expr_elem, 'sawx:expr')
        if len(child_exprs) >= 1:
            # For prompted filters, only extract the sqlExpression, not all text
            child_type = strip_prefix(get_attr(child_exprs[0], '{%s}type' % NAMESPACES['xsi']), 'sawx:')
            if child_type == 'sqlExpression':
                column = text(child_exprs[0])
            elif child_type == 'columnExpression':
                # Extract from nested columnFormula
                formula_elem = find(child_exprs[0], 'saw:columnFormula/sawx:expr')
                if formula_elem is not None:
                    column = text(formula_elem)
                else:
                    column = child_exprs[0].text or ''
            else:
                # Fallback: get only direct text, not nested text
                column = child_exprs[0].text or ''
                if not column:
                    # Try to find first sqlExpression child
                    sql_expr = find(child_exprs[0], './/sawx:expr[@xsi:type="sawx:sqlExpression"]')
                    if sql_expr is not None:
                        column = text(sql_expr)
            
            if column:
                return f"{column} IS PROMPTED"
        return ''
    
    return ''

def parse_global_filter_prompt(xml_path: str) -> Tuple[str, str, List[Dict]]:
    """
    Parse a global filter prompt XML file and extract prompt details.
    Returns: (view_type, instruction, list of prompt dictionaries with column info)
    """
    logger.info(f"Parsing global filter prompt: {xml_path}")
    prompts = []
    view_type = ''
    instruction = ''
    
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Extract view type from root element (e.g., globalFilterPrompt)
        view_type = strip_prefix(get_attr(root, '{%s}type' % NAMESPACES['xsi']), 'saw:')
        
        # Extract subject area
        prompts_elem = find(root, 'saw:prompts')
        subject_area = get_attr(prompts_elem, 'subjectArea', '') if prompts_elem else ''
        
        # Extract instruction from promptStep > instruction > caption > text
        prompt_step = find(root, './/saw:promptStep')
        if prompt_step is not None:
            instruction_elem = find(prompt_step, 'saw:instruction/saw:caption/saw:text')
            if instruction_elem is not None:
                instruction = text(instruction_elem)
        
        # Extract individual prompts
        for prompt in findall(root, './/saw:prompt'):
            prompt_type = strip_prefix(get_attr(prompt, '{%s}type' % NAMESPACES['xsi']), 'saw:')
            column_id = get_attr(prompt, 'columnID', '')
            required = get_attr(prompt, 'required', 'false')
            
            # Extract formula/expression
            # Handle different formula types: sqlExpression, columnExpression
            formula_elem = find(prompt, 'saw:formula/sawx:expr')
            if formula_elem is not None:
                expr_type = strip_prefix(get_attr(formula_elem, '{%s}type' % NAMESPACES['xsi']), 'sawx:')
                if expr_type == 'columnExpression':
                    # For columnExpression, extract the display formula
                    display_formula = find(formula_elem, 'saw:columnFormula[@formulaUse="display"]/sawx:expr')
                    if display_formula is not None:
                        expression = text(display_formula)
                    else:
                        expression = text(formula_elem)
                else:
                    # For sqlExpression or other types
                    expression = text(formula_elem)
            else:
                # Try without namespace prefix
                formula = find(prompt, 'saw:formula')
                if formula is not None:
                    # Get any child element
                    for child in formula:
                        expression = text(child)
                        break
                else:
                    expression = ''
            
            # Extract operator
            operator_elem = find(prompt, 'saw:promptOperator')
            if operator_elem is not None:
                operator = get_attr(operator_elem, 'op', '')
            else:
                operator = ''
            
            # Extract prompt name from label > caption > text
            prompt_name = ''
            label_elem = find(prompt, 'saw:label/saw:caption/saw:text')
            if label_elem is not None:
                prompt_name = text(label_elem)
            
            # Extract UI control type and attributes
            ui_control = find(prompt, 'saw:promptUIControl')
            control_type = ''
            max_choices = ''
            include_all_choices = ''
            if ui_control is not None:
                control_type = strip_prefix(get_attr(ui_control, '{%s}type' % NAMESPACES['xsi']), 'saw:')
                max_choices = get_attr(ui_control, 'maxChoices', '')
                include_all_choices = get_attr(ui_control, 'includeAllChoices', '')
            
            # Extract default values with type and usingCodeValue
            default_values = []
            default_values_elem = find(prompt, 'saw:promptDefaultValues')
            default_values_type = ''
            using_code_value = ''
            if default_values_elem is not None:
                default_values_type = get_attr(default_values_elem, 'type', '')
                using_code_value = get_attr(default_values_elem, 'usingCodeValue', '')
                for default in findall(prompt, 'saw:promptDefaultValues/saw:promptDefaultValue'):
                    default_values.append(text(default))
            
            # Extract constrainPrompt
            constrain_prompt_elem = find(prompt, 'saw:constrainPrompt')
            constrain_prompt_type = ''
            auto_select_value = ''
            if constrain_prompt_elem is not None:
                constrain_prompt_type = get_attr(constrain_prompt_elem, 'type', '')
                auto_select_value = get_attr(constrain_prompt_elem, 'autoSelectValue', '')
            
            # Extract setPromptVariables - now as separate fields
            prompt_var_locations = []
            prompt_var_types = []
            prompt_var_formulas = []
            for var in findall(prompt, 'saw:setPromptVariables/saw:setPromptVariable'):
                var_location = get_attr(var, 'location', '')
                var_type = get_attr(var, 'type', '')
                var_formula = get_attr(var, 'variableFormula', '')
                prompt_var_locations.append(var_location)
                prompt_var_types.append(var_type)
                prompt_var_formulas.append(var_formula)
            
            # Extract promptSource with type, sourceFormula, and promptChoices
            prompt_source_elem = find(prompt, 'saw:promptSource')
            prompt_source_type = ''
            prompt_choices = []
            source_formula = ''
            if prompt_source_elem is not None:
                prompt_source_type = strip_prefix(get_attr(prompt_source_elem, '{%s}type' % NAMESPACES['xsi']), 'saw:')
                
                # For sqlPromptSource, extract sourceFormula attribute
                if prompt_source_type == 'sqlPromptSource':
                    source_formula = get_attr(prompt_source_elem, 'sourceFormula', '')
                
                # For specificChoices or choiceList, extract prompt choices
                if prompt_source_type in ('specificChoices', 'choiceList'):
                    for choice in findall(prompt_source_elem, './/saw:promptChoice'):
                        # Try to get text from caption > text first
                        choice_text = ''
                        choice_text_elem = find(choice, './/saw:caption/saw:text')
                        if choice_text_elem is not None:
                            choice_text = text(choice_text_elem)
                        
                        # If no caption text, try saw:value element
                        if not choice_text:
                            value_elem = find(choice, './/saw:value')
                            if value_elem is not None:
                                choice_text = text(value_elem)
                            else:
                                # Fallback to direct text content
                                choice_text = text(choice)
                        
                        if choice_text:
                            prompt_choices.append(choice_text.strip())
            
            # Parse column name from expression
            column_name = ''
            table_name = ''
            if expression and '"' in expression:
                parts = expression.split('"')
                # parts will be like: ['', 'Time', '.', 'Fiscal Year', '']
                if len(parts) >= 2:
                    table_name = parts[1]
                if len(parts) >= 4:
                    column_name = parts[3]
            
            # Extract UI control styling and formatting information
            style = {}
            layout = {}
            position = {}
            display = {}
            
            # Extract from promptUIControl element
            if ui_control is not None:
                # Get all attributes from promptUIControl
                for attr_name, attr_value in ui_control.attrib.items():
                    clean_name = attr_name.split('}')[-1]
                    if clean_name not in ('type',):  # Skip type as it's already captured
                        style[clean_name] = attr_value
                
                # Extract customWidth if present
                custom_width = find(ui_control, 'saw:customWidth')
                if custom_width is not None:
                    layout['customWidth'] = get_attr(custom_width, 'width', '')
                    layout['customWidthUsing'] = get_attr(custom_width, 'using', '')
                
                # Extract customHeight if present
                custom_height = find(ui_control, 'saw:customHeight')
                if custom_height is not None:
                    layout['customHeight'] = get_attr(custom_height, 'height', '')
                    layout['customHeightUsing'] = get_attr(custom_height, 'using', '')
            
            # Extract from promptStep level (if available)
            prompt_step_parent = find(root, './/saw:promptStep')
            if prompt_step_parent is not None:
                # Check for customWidth at promptStep level
                step_width = find(prompt_step_parent, 'saw:customWidth')
                if step_width is not None:
                    layout['stepCustomWidth'] = get_attr(step_width, 'width', '')
                    layout['stepCustomWidthUsing'] = get_attr(step_width, 'using', '')
            
            # Extract promptSource attributes
            if prompt_source_elem is not None:
                for attr_name, attr_value in prompt_source_elem.attrib.items():
                    clean_name = attr_name.split('}')[-1]
                    if clean_name not in ('type',):
                        display[clean_name] = attr_value
            
            prompts.append({
                'subject_area': subject_area.strip('"'),
                'prompt_type': prompt_type,
                'prompt_name': prompt_name,
                'column_id': column_id,
                'required': required,
                'expression': expression,
                'operator': operator,
                'control_type': control_type,
                'max_choices': max_choices,
                'include_all_choices': include_all_choices,
                'default_values': '|'.join(default_values) if default_values else '',
                'default_values_type': default_values_type,
                'using_code_value': using_code_value,
                'constrain_prompt_type': constrain_prompt_type,
                'auto_select_value': auto_select_value,
                'prompt_var_location': '|'.join(prompt_var_locations) if prompt_var_locations else '',
                'prompt_var_type': '|'.join(prompt_var_types) if prompt_var_types else '',
                'prompt_var_formula': '|'.join(prompt_var_formulas) if prompt_var_formulas else '',
                'prompt_source_type': prompt_source_type,
                'prompt_choices': '|'.join(prompt_choices) if prompt_choices else '',
                'source_formula': source_formula,
                'table_name': table_name,
                'column_name': column_name,
                'instruction': instruction,
                'style': json.dumps(style) if style else '',
                'layout': json.dumps(layout) if layout else '',
                'position': json.dumps(position) if position else '',
                'display': json.dumps(display) if display else ''
            })
            
            # Log detailed information about prompt choices for debugging
            if prompt_choices:
                logger.debug(f"Found {len(prompt_choices)} prompt choices for {prompt_name}: {', '.join(prompt_choices)}")
        
        logger.info(f"Extracted {len(prompts)} prompts from {xml_path}")
    except Exception as exc:
        logger.error(f"Failed to parse global filter prompt '{xml_path}': {exc}", exc_info=True)
    
    return view_type, instruction, prompts

def process_all_reports_recursively(root_dir: str) -> Tuple[Dict, Dict]:
    """
    Recursively scan for report XML files (excluding .atr files and dashboard files) and parse them.
    Returns a dictionary mapping report_path -> report_data
    """
    logger.info(f"Recursively scanning for report XMLs in: {root_dir}")
    
    reports_map = {}
    prompts_map = {}
    
    if not os.path.isdir(root_dir):
        logger.warning(f"Directory not found: {root_dir}")
        return reports_map
    
    # Walk through all directories recursively
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for fname in filenames:
            # Skip .atr files and dashboard-related files
            if fname.endswith('.atr') or 'page+' in fname.lower():
                continue
            
            # Process XML files (with or without extension)
            fpath = os.path.join(dirpath, fname)
            
            # Get relative path from root for report_path
            rel_path = os.path.relpath(fpath, root_dir)
            # Convert to catalog path format: /shared/... 
            # First replace + with spaces, then URL-decode to handle %2e, %2f, etc.
            report_path = '/' + rel_path.replace(os.sep, '/').replace('+', ' ')
            report_path = unquote(report_path)
            # Remove file extension if present
            if report_path.endswith('.xml'):
                report_path = report_path[:-4]
            
            # Try to parse as global filter prompt first (check filename or if in prompts directory)
            if 'prompt' in fname.lower() or 'prompt' in dirpath.lower():
                try:
                    # Attempting to parse prompt file
                    view_type, instruction, prompt_data = parse_global_filter_prompt(fpath)
                    if prompt_data:
                        prompts_map[report_path] = {
                            'file_path': fpath,
                            'prompt_path': report_path,
                            'view_type': view_type,
                            'instruction': instruction,
                            'prompt_data': prompt_data
                        }
                        logger.info(f"Parsed prompt: {report_path}")
                        continue
                except Exception as exc:
                    # Not a prompt file
                    pass
            
            # Try to parse as report XML
            # Skip dashboard layout/page files but allow reports with "dashboard" in the name
            if 'dashboard+layout' not in fname.lower() and not fname.lower().startswith('page+'):
                try:
                    # Attempting to parse report file
                    report_data = parse_report(fpath)
                    
                    if report_data:
                        reports_map[report_path] = {
                            'file_path': fpath,
                            'report_path': report_path,
                            'report_data': report_data
                        }
                        logger.info(f"Parsed report: {report_path}")
                except Exception as exc:
                    # Skipping non-report file
                    continue
    
    logger.info(f"Found and parsed {len(reports_map)} reports and {len(prompts_map)} prompts")
    return reports_map, prompts_map

def create_dashboard_csv_data(agg_dashboard_report_views, agg_dashboard_global_filters, agg_dashboard_action_links):
    """
    Create Dashboard CSV with standardized naming and paths
    """
    dashboard_rows = []
    
    # Add report views
    for rv in agg_dashboard_report_views:
        page_file = rv.get('page_file', '')
        worksheet_path = file_path_to_catalog_path(page_file, os.path.join(os.path.dirname(__file__), '..', 'input_xml')) if page_file else ''
        
        dashboard_rows.append({
            'WorksheetName': rv.get('page_name', ''),
            'DashboardName': unquote(rv.get('dashboard_name', '').replace('+', ' ').split("/")[-1]),
            'ObjectName': rv.get('caption', ''),
            'ObjectType': 'Reports',
            'ObjectPath': rv.get('report_path', ''),
            'WorksheetPath': worksheet_path,
            'DashboardPath': dashboard_name_to_catalog_path(rv.get('dashboard_name', '')),
        })
    
    # Add global filters/prompts
    for gf in agg_dashboard_global_filters:
        page_file = gf.get('page_file', '')
        worksheet_path = file_path_to_catalog_path(page_file, os.path.join(os.path.dirname(__file__), '..', 'input_xml')) if page_file else ''
        
        dashboard_rows.append({
            'WorksheetName': gf.get('page_name', ''),
            'DashboardName': unquote(gf.get('dashboard_name', '').replace('+', ' ').split("/")[-1]),
            'ObjectName': gf.get('caption', ''),
            'ObjectType': 'Prompts',
            'ObjectPath': gf.get('filter_path', ''),
            'WorksheetPath': worksheet_path,
            'DashboardPath': dashboard_name_to_catalog_path(gf.get('dashboard_name', '')),
        })
    
    # Add action links
    for al in agg_dashboard_action_links:
        page_file = al.get('page_file', '')
        worksheet_path = file_path_to_catalog_path(page_file, os.path.join(os.path.dirname(__file__), '..', 'input_xml')) if page_file else ''
        
        dashboard_rows.append({
            'WorksheetName': al.get('page_name', ''),
            'DashboardName':  unquote(al.get('dashboard_name', '').replace('+', ' ').split("/")[-1]),
            'ObjectName': al.get('caption', ''),
            'ObjectType': 'Links',
            'ObjectPath': al.get('navigation_path', ''),
            'WorksheetPath': worksheet_path,
            'DashboardPath': dashboard_name_to_catalog_path(al.get('dashboard_name', '')),
        })
    
    return dashboard_rows

def create_worksheets_csv_data(agg_dashboard_report_views, reports_map):
    """
    Create Worksheets CSV with comprehensive dashboard page + report column details
    Includes X/Y axis info, derived column sources, and proper table/column list extraction
    """
    worksheet_rows = []
    
    # Create case-insensitive lookup maps (full path and basename fallbacks)
    reports_map_lower = {k.lower(): v for k, v in reports_map.items()}
    reports_by_basename_lower = {}
    try:
        for k, v in reports_map.items():
            base = os.path.basename(k.strip('/')).lower()
            if base and base not in reports_by_basename_lower:
                reports_by_basename_lower[base] = v
    except Exception:
        pass
    
    for rv in agg_dashboard_report_views:
        page_name = rv.get('page_name', '')
        report_name = rv.get('caption', '')
        report_path = rv.get('report_path', '')
        report_view_name = rv.get('report_view_name', '')
        dashboard_name = rv.get('dashboard_name', '').replace('+', ' ')
        dashboard_path = rv.get('dashboard_name', '')
        page_file = rv.get('page_file', '')
        worksheet_path = file_path_to_catalog_path(page_file, os.path.join(os.path.dirname(__file__), '..', 'input_xml')) if page_file else ''
        
        # Find matching report in reports_map (case-insensitive)
        # URL-decode the report path to handle encoded characters like %2e (.)
        decoded_report_path = unquote(report_path)
        report_info = reports_map_lower.get(decoded_report_path.lower())
        if not report_info:
            # Fallback: match by last segment (report name) ignoring folders and case
            base = os.path.basename(decoded_report_path.strip('/')).lower()
            report_info = reports_by_basename_lower.get(base)
        
        if report_info:
            report_data = report_info.get('report_data', ())
            if len(report_data) >= 7:
                report_rows, column_rows, column_order_rows, view_rows, edge_rows, edge_layer_rows, chart_rows = report_data[0:7]
                
                # Get additional data if available
                chart_category_rows = report_data[7] if len(report_data) > 7 else []
                chart_measure_rows = report_data[8] if len(report_data) > 8 else []
                pivot_measures_rows = report_data[9] if len(report_data) > 9 else []
                
                # Get subject area from report
                subject_area = ''
                if report_rows:
                    subject_area = report_rows[0].get('subject_area', '').strip('"')
                
                # Create column lookup
                col_map = {col.get('column_id', ''): col for col in column_rows}
                
                # Determine which columns are in X/Y axis based on chart measures and categories
                x_axis_cols = set(cat.get('column_id', '') for cat in chart_category_rows)
                y_axis_cols = {}  # col_id -> (measure_type, riser_type)
                for measure in chart_measure_rows:
                    col_id = measure.get('column_id', '')
                    y_axis_cols[col_id] = (measure.get('measure_type', ''), measure.get('riser_type', ''))

                # Build aggregation rule lookup per (view_name, column_id)
                # Combine from pivot measures list and edge layer specifications
                agg_by_map: Dict[Tuple[str, str], str] = {}
                try:
                    for pm in pivot_measures_rows:
                        key = (pm.get('view_name', ''), pm.get('column_id', ''))
                        agg = pm.get('agg_rule', '')
                        if agg:
                            agg_by_map[key] = agg
                except Exception:
                    pass
                try:
                    for el in edge_layer_rows:
                        key = (el.get('view_name', ''), el.get('column_id', ''))
                        agg = el.get('agg_rule', '')
                        if agg:
                            agg_by_map[key] = agg
                except Exception:
                    pass

                normalized_view_name = normalize_view_name(report_view_name)

                # Determine ViewIds to emit for worksheet rows so they match ChartType.csv exactly.
                # New rule: ALWAYS include all non-compound saw:view elements (i.e., exclude only the compoundView itself),
                # in document order. This ensures pivotTableView!1, pivotTableView!2, etc. are always populated.
                ids_to_emit: List[str] = []
                try:
                    for v in view_rows:
                        if v.get('view_xsi_type', '') != 'compoundView':
                            raw = v.get('view_name_raw', '')
                            if raw:
                                ids_to_emit.append(raw)
                    # De-dupe while preserving order (defensive)
                    seen = set()
                    ids_to_emit = [x for x in ids_to_emit if not (x in seen or seen.add(x))]
                except Exception:
                    ids_to_emit = []

                # Create row for each column in the report, for each desired ViewId
                for col in column_rows:
                    col_id = col.get('column_id', '')
                    expression = col.get('expression', '')
                    col_type = col.get('column_xsi_type', '')
                    expr_type = col.get('expr_xsi_type', '')
                    
                    # Parse expression to extract ALL table and column names (for formulas with multiple tables)
                    # Use a strict regex to match only patterns of the form "Table"."Column"
                    table_names: List[str] = []
                    column_names: List[str] = []
                    if expression:
                        try:
                            # Find all pairs of "Table"."Column"
                            pairs = re.findall(r'"([^"\\]+)"\s*\.\s*"([^"\\]+)"', expression)
                            if pairs:
                                # Preserve order, but deduplicate while maintaining first occurrence
                                seen_pairs = set()
                                for t, c in pairs:
                                    if (t, c) not in seen_pairs:
                                        seen_pairs.add((t, c))
                                        table_names.append(t)
                                        column_names.append(c)
                        except Exception:
                            # If regex fails for any reason, fall back to headings below
                            table_names = []
                            column_names = []
                    
                    # Handle VALUEOF expressions like VALUEOF(NQ_SESSION.SAW_DASHBOARD)
                    if 'VALUEOF' in expression.upper() and '.' in expression:
                        # Extract content within VALUEOF()
                        valueof_match = re.search(r'VALUEOF\s*\(\s*([^)]+)\s*\)', expression, re.IGNORECASE)
                        if valueof_match:
                            valueof_content = valueof_match.group(1)
                            # Split by . to get table.column
                            if '.' in valueof_content:
                                parts = valueof_content.split('.')
                                if len(parts) >= 2:
                                    table_names.append(parts[0].strip())
                                    column_names.append(parts[1].strip())
                    
                    # If no table/column names extracted from expression, use headings
                    if not table_names and not column_names:
                        table_heading = col.get('table_heading', '')
                        column_heading = col.get('column_heading', '')
                        if table_heading:
                            table_names.append(table_heading)
                        if column_heading:
                            column_names.append(column_heading)
                    
                    # Determine X/Y axis info
                    x_axis = 0
                    y_axis = 0
                    measure_type = ''
                    riser_type = ''
                    encoding = ''
                    # Prefer aggRule from the specific view if available; otherwise fall back to any view
                    summerized_by = agg_by_map.get((normalized_view_name, col_id), '')
                    if not summerized_by and not normalized_view_name:
                        # No specific view context (synthesized case): pick any non-empty agg for this column
                        try:
                            for (v_name, c_id), agg in agg_by_map.items():
                                if c_id == col_id and agg:
                                    summerized_by = agg
                                    break
                        except Exception:
                            summerized_by = ''
                    
                    if col_id in x_axis_cols:
                        x_axis = 1
                        encoding = 'category'
                    
                    if col_id in y_axis_cols:
                        y_axis = 1
                        measure_type, riser_type = y_axis_cols[col_id]
                        encoding = riser_type if riser_type else 'measure'
                    
                    # For derived columns, identify source columns
                    source_column_ids: List[str] = []
                    source_expressions: List[str] = []
                    is_derived = expr_type not in ['sqlExpression', ''] or 'CASE' in expression.upper() or '(' in expression
                    
                    if is_derived:
                        # Try to extract column IDs referenced in the expression
                        # Look for patterns like saw_0, saw_1, etc. or other column references
                        col_refs = re.findall(r'saw_\d+', expression)
                        # Deduplicate and sort numerically by the integer suffix for deterministic order
                        unique_refs = sorted(set(col_refs), key=lambda s: int(s.split('_')[1]) if '_' in s and s.split('_')[1].isdigit() else 0)
                        source_column_ids = unique_refs
                        # Map expressions in the same sorted order
                        for src_id in source_column_ids:
                            src_col = col_map.get(src_id, {})
                            source_expressions.append(src_col.get('expression', ''))
                    
                    # Build fields
                    table_names_joined = '|'.join(table_names) if table_names else ''
                    # Skip any row where TableNames mistakenly contains IFERROR per requirement
                    if 'IFERROR' in table_names_joined.upper():
                        continue

                    # Additional filter: skip rows if TableNames contains any banned table names
                    # Compare on individual table tokens, case-insensitive
                    if table_names:
                        banned_tables = {
                            'volume',
                            'key reporting fields',
                            'actual ship date additional information',
                            'invoice date additional information',
                            'scheduled pick date additional information',
                        }
                        normalized_tables = {t.strip().lower() for t in table_names}
                        if normalized_tables & banned_tables:
                            continue

                    # Additional filter: skip rows if the exact union of TableNames matches banned unions
                    # Compare case-insensitively on the full pipe-joined string
                    if table_names_joined:
                        banned_unions = {
                            'scheduled pick date|transaction details|as of date',
                        }
                        if table_names_joined.strip().lower() in banned_unions:
                            continue

                    for emit_view_id in ids_to_emit or ['']:
                        worksheet_rows.append({
                            'WorksheetName': page_name,
                            'DashboardName': unquote(dashboard_name.replace('+', ' ').split("/")[-1]),
                            'ReportName': report_name,
                            'ReportNameTag': report_view_name,
                            'ViewId': emit_view_id,
                            'DataSourceName': subject_area,
                            'TableNames': table_names_joined,
                            'ColumnNames': '|'.join(column_names) if column_names else '',
                            'Formula': expression,
                            'ColumnId': col_id,
                            'ColumnType': col_type,
                            'ExpressionType': expr_type,
                            'X': x_axis,
                            'Y': y_axis,
                            'Encoding': encoding,
                            'MeasureType': measure_type,
                            'RiserType': riser_type,
                            'Summerized_by': summerized_by,
                            'IsDerived': 'Yes' if is_derived else 'No',
                            'SourceColumnIds': '|'.join(source_column_ids) if source_column_ids else '',
                            'SourceExpressions': '|'.join(source_expressions) if source_expressions else '',
                            'WorksheetPath': worksheet_path,
                            'DashboardPath': dashboard_name_to_catalog_path(dashboard_path),
                            'ReportPath': report_path,
                        })
        else:
            # Report not found, create placeholder row
            worksheet_rows.append({
                'WorksheetName': page_name,
                'DashboardName': unquote(dashboard_name.replace('+', ' ').split("/")[-1]),
                'ReportName': report_name,
                'ReportNameTag': report_view_name,
                'ViewId': '',
                'DataSourceName': '',
                'TableNames': '',
                'ColumnNames': '',
                'Formula': '',
                'ColumnId': '',
                'ColumnType': '',
                'ExpressionType': '',
                'X': 0,
                'Y': 0,
                'Encoding': '',
                'MeasureType': '',
                'RiserType': '',
                'Summerized_by': '',
                'IsDerived': '',
                'SourceColumnIds': '',
                'SourceExpressions': '',
                'WorksheetPath': worksheet_path,
                'DashboardPath': dashboard_path,
                'ReportPath': report_path,
            })
    
    return worksheet_rows

def create_charttype_csv_data(agg_dashboard_report_views, reports_map):
    """
    Create ChartType CSV - extract only views that are in compound view, with correct chart types
    """
    charttype_rows = []
    
    # Create case-insensitive lookup maps (full path and basename fallbacks)
    reports_map_lower = {k.lower(): v for k, v in reports_map.items()}
    reports_by_basename_lower = {}
    try:
        for k, v in reports_map.items():
            base = os.path.basename(k.strip('/')).lower()
            if base and base not in reports_by_basename_lower:
                reports_by_basename_lower[base] = v
    except Exception:
        pass
    
    for rv in agg_dashboard_report_views:
        page_name = rv.get('page_name', '')
        report_name = rv.get('caption', '')
        report_path = rv.get('report_path', '')
        report_view_name = rv.get('report_view_name', '')
        dashboard_name = rv.get('dashboard_name', '').replace('+', ' ')
        dashboard_path = rv.get('dashboard_name', '')
        page_file = rv.get('page_file', '')
        worksheet_path = file_path_to_catalog_path(page_file, os.path.join(os.path.dirname(__file__), '..', 'input_xml')) if page_file else ''
        
        # Find matching report (case-insensitive)
        # URL-decode the report path to handle encoded characters like %2e (.)
        decoded_report_path = unquote(report_path)
        report_info = reports_map_lower.get(decoded_report_path.lower())
        if not report_info:
            base = os.path.basename(decoded_report_path.strip('/')).lower()
            report_info = reports_by_basename_lower.get(base)
        
        if report_info:
            report_data = report_info.get('report_data', ())
            if len(report_data) >= 7:
                view_rows = report_data[3]
                chart_rows = report_data[6]
                
                # Find the compound view and get its current view
                compound_view = None
                # Track the compound's current view name (raw view name)
                current_view_idx = '0'
                for view in view_rows:
                    if view.get('view_xsi_type') == 'compoundView':
                        compound_view = view
                        current_view_idx = view.get('current_view', '0')
                        break
                
                # If compound view exists, include ALL non-compound views (not just compound members)
                if compound_view:
                    # Resolve selected child RAW name from compound children order using numeric current_view index
                    selected_child_name_raw = ''
                    try:
                        order_raw = compound_view.get('compound_children_order_raw', []) or []
                        idx_str = compound_view.get('current_view', '0')
                        if str(idx_str).isdigit():
                            idx = int(idx_str)
                            if 0 <= idx < len(order_raw):
                                selected_child_name_raw = order_raw[idx]
                    except Exception:
                        selected_child_name_raw = ''
                    # Process all non-compound views in document order
                    for view in view_rows:
                        view_type = view.get('view_xsi_type', '')
                        view_name = view.get('view_name', '')
                        view_name_raw = view.get('view_name_raw', view_name)
                        
                        # Skip the compound view itself
                        if view_type == 'compoundView':
                            continue
                        
                        # Determine chart type based on view type
                        if view_type == 'dvtchart':
                            # Get actual chart type from chart_rows
                            chart_info = next((c for c in chart_rows if c.get('view_name') == view_name), None)
                            if chart_info:
                                display_type = chart_info.get('display_type', 'chart')
                                display_subtype = chart_info.get('display_subtype', '')
                                chart_type = f"{display_type}_{display_subtype}" if display_subtype else display_type
                            else:
                                chart_type = 'chart'
                        elif view_type == 'tableView':
                            chart_type = 'table'
                        elif view_type == 'pivotTableView':
                            chart_type = 'pivot'
                        elif view_type == 'titleView':
                            chart_type = 'title'
                        else:
                            chart_type = view_type
                        
                        # Extract title text for titleView
                        title_text = ''
                        if view_type == 'titleView':
                            # Title text would be in the view, but we need to parse the report XML for it
                            # For now, leave empty - would need additional parsing
                            title_text = ''
                        
                        charttype_rows.append({
                            'WorksheetName': page_name,
                            'DashboardName': unquote(dashboard_name.replace('+', ' ').split("/")[-1]),
                            'ReportName': report_name,
                            'ReportNameTag': report_view_name,
                            'ViewId': view_name_raw,
                            'ViewType': view_type,
                            'ChartType': chart_type,
                            'TitleText': title_text,
                            'IsCurrentView': 'Yes' if view_name_raw == selected_child_name_raw else 'No',
                            'Reason': '',
                            'WorksheetPath': worksheet_path,
                            'DashboardPath': dashboard_name_to_catalog_path(dashboard_path),
                            'ReportPath': report_path,
                        })
                else:
                    # No compound view, just list all views
                    for view in view_rows:
                        view_type = view.get('view_xsi_type', '')
                        view_name = view.get('view_name', '')
                        view_name_raw = view.get('view_name_raw', view_name)
                        
                        if view_type == 'dvtchart':
                            chart_info = next((c for c in chart_rows if c.get('view_name') == view_name), None)
                            if chart_info:
                                display_type = chart_info.get('display_type', 'chart')
                                display_subtype = chart_info.get('display_subtype', '')
                                chart_type = f"{display_type}_{display_subtype}" if display_subtype else display_type
                            else:
                                chart_type = 'chart'
                        elif view_type == 'tableView':
                            chart_type = 'table'
                        elif view_type == 'pivotTableView':
                            chart_type = 'pivot'
                        elif view_type == 'titleView':
                            chart_type = 'title'
                        else:
                            chart_type = view_type
                        
                        charttype_rows.append({
                            'WorksheetName': page_name,
                            'DashboardName': unquote(dashboard_name.replace('+', ' ').split("/")[-1]),
                            'ReportName': report_name,
                            'ReportNameTag': report_view_name,
                            'ViewId': view_name_raw,
                            'ViewType': view_type,
                            'ChartType': chart_type,
                            'TitleText': '',
                            'IsCurrentView': 'No',
                            'Reason': '',
                            'WorksheetPath': worksheet_path,
                            'DashboardPath': dashboard_name_to_catalog_path(dashboard_path),
                            'ReportPath': report_path,
                        })
        else:
            # Report not found
            charttype_rows.append({
                'WorksheetName': page_name,
                'DashboardName': unquote(dashboard_name.replace('+', ' ').split("/")[-1]),
                'ReportName': report_name,
                'ReportNameTag': report_view_name,
                'ViewName': '',
                'ViewType': '',
                'ChartType': 'unknown',
                'TitleText': '',
                'IsCurrentView': 'No',
                'Reason': f'Report not found at path: {report_path}',
                'WorksheetPath': worksheet_path,
                'DashboardPath': dashboard_path,
                'ReportPath': report_path,
            })
    
    return charttype_rows

def file_path_to_catalog_path(file_path: str, root_dir: str) -> str:
    """
    Convert a file system path to a catalog path format starting from /shared/.
    Dynamically finds the 'shared' folder in the path hierarchy.
    E.g., C:/...input_xml/shared/page+1 -> /shared/page 1
    """
    if not file_path:
        return ''
    try:
        # Normalize paths
        file_path = os.path.normpath(file_path)
        root_dir = os.path.normpath(root_dir)
        
        # Get relative path from root
        rel_path = os.path.relpath(file_path, root_dir)
        
        # Split path into parts
        path_parts = rel_path.split(os.sep)
        
        # Find 'shared' in the path (case-insensitive)
        shared_idx = -1
        for i, part in enumerate(path_parts):
            if part.lower() == 'shared':
                shared_idx = i
                break
        
        # If 'shared' found, construct path from there
        if shared_idx >= 0:
            catalog_parts = path_parts[shared_idx:]
        else:
            # If no 'shared' folder, use the full relative path
            catalog_parts = path_parts
        
        # Join with / and replace + with space
        catalog_path = '/' + '/'.join(catalog_parts).replace('+', ' ')
        
        # Remove .xml extension if present
        if catalog_path.endswith('.xml'):
            catalog_path = catalog_path[:-4]
        
        return catalog_path
    except Exception as e:
        logger.warning(f"Failed to convert path {file_path}: {e}")
        return ''

def dashboard_name_to_catalog_path(dashboard_name: str) -> str:
    """
    Convert dashboard name (e.g., 'shared/hr/_portal/hr+dashboard') to catalog path format.
    Returns path starting with /shared/ and spaces instead of +
    """
    if not dashboard_name:
        return ''
    # Replace + with space and ensure it starts with /
    path = dashboard_name.replace('+', ' ')
    if not path.startswith('/'):
        path = '/' + path
    return path

def create_filters_csv_data(agg_dashboard_report_views, agg_dashboard_global_filters, reports_map, prompts_map):
    """
    Create Filters CSV with detailed column information and prompt details
    """
    filter_rows = []
    
    # Create case-insensitive lookup maps (full path and basename fallbacks)
    reports_map_lower = {k.lower(): v for k, v in reports_map.items()}
    reports_by_basename_lower = {}
    try:
        for k, v in reports_map.items():
            base = os.path.basename(k.strip('/')).lower()
            if base and base not in reports_by_basename_lower:
                reports_by_basename_lower[base] = v
    except Exception:
        pass
    prompts_map_lower = {k.lower(): v for k, v in prompts_map.items()}
    
    # Add filters from reports
    for rv in agg_dashboard_report_views:
        page_name = rv.get('page_name', '')
        report_name = rv.get('caption', '')
        report_path = rv.get('report_path', '')
        page_file = rv.get('page_file', '')
        # Convert file path to catalog path
        worksheet_path = file_path_to_catalog_path(page_file, os.path.join(os.path.dirname(__file__), '..', 'input_xml')) if page_file else ''
        dashboard_name = rv.get('dashboard_name', '').replace('+', ' ')
        dashboard_path = rv.get('dashboard_name', '')
        
        # URL-decode the report path to handle encoded characters like %2e (.)
        decoded_report_path = unquote(report_path)
        report_info = reports_map_lower.get(decoded_report_path.lower())
        if not report_info:
            base = os.path.basename(decoded_report_path.strip('/')).lower()
            report_info = reports_by_basename_lower.get(base)
        
        if report_info:
            report_data = report_info.get('report_data', ())
            if len(report_data) >= 3:
                report_rows = report_data[0]
                column_rows = report_data[1]
                column_order_rows = report_data[2]
                
                # Create column lookup
                col_map = {col.get('column_id', ''): col for col in column_rows}
                
                # Extract report-level filters from criteria/filter
                # Parse all filter expressions including complex logical operators
                try:
                    file_path = report_info.get('file_path', '')
                    if file_path and os.path.exists(file_path):
                        tree = ET.parse(file_path)
                        root = tree.getroot()
                        
                        # Extract criteria-level attributes for formatting
                        criteria_elem = find(root, './/saw:criteria')
                        criteria_attrs = {}
                        if criteria_elem is not None:
                            for attr_name, attr_value in criteria_elem.attrib.items():
                                clean_name = attr_name.split('}')[-1]
                                if clean_name not in ('type',):
                                    criteria_attrs[clean_name] = attr_value
                        
                        # Find the main filter expression
                        filter_elem = find(root, './/saw:criteria/saw:filter')
                        main_filter_expr = find(filter_elem, 'sawx:expr') if filter_elem is not None else None
                        
                        # Extract filter-level attributes
                        filter_attrs = {}
                        if filter_elem is not None:
                            for attr_name, attr_value in filter_elem.attrib.items():
                                clean_name = attr_name.split('}')[-1]
                                filter_attrs[clean_name] = attr_value
                        
                        if main_filter_expr is not None:
                            # Parse all filter expressions recursively
                            parsed_filters = parse_filter_expression(main_filter_expr)
                            
                            for filter_info in parsed_filters:
                                # Build style/layout info from criteria and filter attributes
                                style_info = filter_attrs.copy()
                                layout_info = criteria_attrs.copy()
                                
                                # Extract subjectArea to separate column, remove withinHierarchy
                                subject_area = layout_info.pop('subjectArea', '').strip('"')
                                # Remove withinHierarchy from layout (not needed in output)
                                layout_info.pop('withinHierarchy', '')
                                
                                filter_rows.append({
                                    'WorksheetName': page_name,
                                    'DashboardName': unquote(dashboard_name.replace('+', ' ').split("/")[-1]),
                                    'ReportName': report_name,
                                    'FilterType': 'ReportFilter',
                                    'ColumnId': '',
                                    'ColumnName': filter_info.get('column_name', ''),
                                    'TableName': filter_info.get('table_name', ''),
                                    'Direction': '',
                                    'Expression': filter_info.get('column_expression', ''),
                                    'Operator': filter_info.get('operator', ''),
                                    'ParentOperator': filter_info.get('parent_operator', ''),
                                    'FilterValue': filter_info.get('filter_value', ''),
                                    'SubjectArea': subject_area,
                                    'Style': json.dumps(style_info) if style_info else '',
                                    'Layout': json.dumps(layout_info) if layout_info else '',
                                    'Position': '',
                                    'Display': '',
                                    'WorksheetPath': worksheet_path,
                                    'DashboardPath': dashboard_name_to_catalog_path(dashboard_path),
                                    'ReportPath': report_path,
                                })
                except Exception as exc:
                    logger.warning(f"Failed to extract report filters from {file_path}: {exc}")
                
                # Extract filters from column orders (sort filters)
                for order in column_order_rows:
                    col_id = order.get('column_id', '')
                    col_info = col_map.get(col_id, {})
                    expression = col_info.get('expression', '')
                    
                    # Parse column name from expression
                    column_name = ''
                    table_name = ''
                    if expression and '"' in expression:
                        parts = expression.split('"')
                        if len(parts) >= 2:
                            table_name = parts[1]
                        if len(parts) >= 4:
                            column_name = parts[3]
                    
                    # Extract sort order attributes
                    sort_attrs = {}
                    for key, value in order.items():
                        if key not in ('column_id', 'direction'):
                            sort_attrs[key] = value
                    
                    filter_rows.append({
                        'WorksheetName': page_name,
                        'DashboardName': unquote(dashboard_name.replace('+', ' ').split("/")[-1]),
                        'ReportName': report_name,
                        'FilterType': 'Sort',
                        'ColumnId': col_id,
                        'ColumnName': column_name,
                        'TableName': table_name,
                        'Direction': order.get('direction', ''),
                        'Expression': expression,
                        'Style': json.dumps(sort_attrs) if sort_attrs else '',
                        'Layout': '',
                        'Position': '',
                        'Display': '',
                        'WorksheetPath': worksheet_path,
                        'DashboardPath': dashboard_name_to_catalog_path(dashboard_path),
                        'ReportPath': report_path,
                    })
    
    # Add global filters/prompts from dashboard with detailed prompt info
    for gf in agg_dashboard_global_filters:
        filter_path = gf.get('filter_path', '')
        page_name = gf.get('page_name', '')
        page_file = gf.get('page_file', '')
        worksheet_path = file_path_to_catalog_path(page_file, os.path.join(os.path.dirname(__file__), '..', 'input_xml')) if page_file else ''
        dashboard_name = gf.get('dashboard_name', '').replace('+', ' ')
        dashboard_path = gf.get('dashboard_name', '')
        
        # Extract dashboard page layout information
        section_name = gf.get('section_name', '')
        column_name = gf.get('column_name', '')
        filter_index = gf.get('filter_index', '')
        
        # Build position info from dashboard page structure
        position_info = {
            'columnName': column_name,
            'sectionName': section_name,
            'filterIndex': str(filter_index)
        }
        
        # Look up prompt details (try both original and URL-decoded versions)
        prompt_info = prompts_map_lower.get(filter_path.lower())
        if not prompt_info:
            # Try URL-decoding the filter_path (e.g., "FIN 3.1.3" -> "fin 3%2e1%2e3")
            # We need to URL-encode it to match the parsed paths
            filter_path_encoded = filter_path.replace('.', '%2e').replace(' ', '+')
            prompt_info = prompts_map_lower.get(filter_path_encoded.lower())
        
        if prompt_info:
            view_type = prompt_info.get('view_type', '')
            instruction = prompt_info.get('instruction', '')
            prompt_data_list = prompt_info.get('prompt_data', [])
            for prompt_data in prompt_data_list:
                # Merge position info with prompt position data
                combined_position = position_info.copy()
                if prompt_data.get('position'):
                    try:
                        prompt_pos = json.loads(prompt_data.get('position', '{}'))
                        combined_position.update(prompt_pos)
                    except:
                        pass
                
                # Extract filter value from default values for global filter prompts
                filter_value = prompt_data.get('default_values', '')
                operator = prompt_data.get('operator', '')
                expression = prompt_data.get('expression', '')
                column_name = prompt_data.get('column_name', '')
                table_name = prompt_data.get('table_name', '')
                
                # If expression is incomplete for IN operator, reconstruct it
                if operator == 'in' and expression and 'IN' not in expression.upper() and table_name and column_name:
                    if filter_value:
                        expression = f'"{table_name}"."{column_name}" IN ({filter_value})'
                    else:
                        expression = f'"{table_name}"."{column_name}" IN ()'
                
                filter_row = {
                    'WorksheetName': page_name,
                    'DashboardName': unquote(dashboard_name.replace('+', ' ').split("/")[-1]),
                    'ReportName': '',  # Empty for global filters as requested
                    'FilterType': view_type if view_type else 'Prompt',  # Use actual view type from XML
                    'PromptType': prompt_data.get('prompt_type', ''),
                    'PromptName': prompt_data.get('prompt_name', ''),
                    'Formula': expression,
                    'ColumnId': prompt_data.get('column_id', ''),
                    'ColumnName': column_name,
                    'TableName': table_name,
                    'Direction': '',
                    'Expression': expression,
                    'Operator': operator,
                    'ParentOperator': '',
                    'FilterValue': filter_value,
                    'ControlType': prompt_data.get('control_type', ''),
                    'MaxChoices': prompt_data.get('max_choices', ''),
                    'IncludeAllChoices': prompt_data.get('include_all_choices', ''),
                    'Required': prompt_data.get('required', ''),
                    'DefaultValues': prompt_data.get('default_values', ''),
                    'DefaultValuesType': prompt_data.get('default_values_type', ''),
                    'UsingCodeValue': prompt_data.get('using_code_value', ''),
                    'ConstrainPromptType': prompt_data.get('constrain_prompt_type', ''),
                    'AutoSelectValue': prompt_data.get('auto_select_value', ''),
                    'PromptVarLocation': prompt_data.get('prompt_var_location', ''),
                    'PromptVarType': prompt_data.get('prompt_var_type', ''),
                    'PromptVarFormula': prompt_data.get('prompt_var_formula', ''),
                    'PromptSourceType': prompt_data.get('prompt_source_type', ''),
                    'PromptChoices': prompt_data.get('prompt_choices', ''),
                    'SourceFormula': prompt_data.get('source_formula', ''),
                    'Instruction': prompt_data.get('instruction', ''),
                    'SubjectArea': prompt_data.get('subject_area', ''),
                    'Style': prompt_data.get('style', ''),
                    'Layout': prompt_data.get('layout', ''),
                    'Position': json.dumps(combined_position) if combined_position else '',
                    'Display': prompt_data.get('display', ''),
                    'WorksheetPath': worksheet_path,
                    'DashboardPath': dashboard_path,
                    'ReportPath': filter_path,
                }
                filter_rows.append(filter_row)
        else:
            # Fallback if prompt not parsed
            filter_rows.append({
                'WorksheetName': page_name,
                'DashboardName': unquote(dashboard_name.replace('+', ' ').split("/")[-1]),
                'ReportName': '',  # Empty for global filters
                'FilterType': 'Prompt',
                'ColumnId': '',
                'ColumnName': gf.get('caption', ''),
                'TableName': '',
                'Direction': '',
                'Expression': '',
                'WorksheetPath': worksheet_path,
                'DashboardPath': dashboard_path,
                'ReportPath': filter_path,
            })
    
    return filter_rows

def create_windows_csv_data(agg_dashboard_report_views, agg_dashboard_global_filters, agg_dashboard_action_links):
    """
    Create Windows CSV with standardized naming and paths
    """
    window_rows = []
    
    # Add report views as windows
    for idx, rv in enumerate(agg_dashboard_report_views):
        page_file = rv.get('page_file', '')
        worksheet_path = file_path_to_catalog_path(page_file, os.path.join(os.path.dirname(__file__), '..', 'input_xml')) if page_file else ''
        
        window_rows.append({
            'WorksheetName': rv.get('page_name', ''),
            'DashboardName': unquote(rv.get('dashboard_name', '').replace('+', ' ').split("/")[-1]),
            'WindowName': rv.get('caption', ''),
            'WindowClass': 'Reports',
            'SectionName': rv.get('section_name', ''),
            'ColumnName': rv.get('column_name', ''),
            'Display': rv.get('display', ''),
            'YPosition': idx * 400,
            'WorksheetPath': worksheet_path,
            'DashboardPath': dashboard_name_to_catalog_path(rv.get('dashboard_name', '')),
        })
    
    # Add global filters as windows
    for idx, gf in enumerate(agg_dashboard_global_filters):
        page_file = gf.get('page_file', '')
        worksheet_path = file_path_to_catalog_path(page_file, os.path.join(os.path.dirname(__file__), '..', 'input_xml')) if page_file else ''
        
        window_rows.append({
            'WorksheetName': gf.get('page_name', ''),
            'DashboardName': unquote(gf.get('dashboard_name', '').replace('+', ' ').split("/")[-1]),
            'WindowName': gf.get('caption', ''),
            'WindowClass': 'Prompts',
            'SectionName': gf.get('section_name', ''),
            'ColumnName': gf.get('column_name', ''),
            'Display': '',
            'YPosition': idx * 100,
            'WorksheetPath': worksheet_path,
            'DashboardPath': dashboard_name_to_catalog_path(gf.get('dashboard_name', '')),
        })
    
    # Add action links as windows
    for idx, al in enumerate(agg_dashboard_action_links):
        page_file = al.get('page_file', '')
        worksheet_path = file_path_to_catalog_path(page_file, os.path.join(os.path.dirname(__file__), '..', 'input_xml')) if page_file else ''
        
        window_rows.append({
            'WorksheetName': al.get('page_name', ''),
            'DashboardName': unquote(al.get('dashboard_name', '').replace('+', ' ').split("/")[-1]),
            'WindowName': al.get('caption', ''),
            'WindowClass': 'Links',
            'SectionName': al.get('section_name', ''),
            'ColumnName': al.get('column_name', ''),
            'Display': '',
            'YPosition': idx * 50,
            'WorksheetPath': worksheet_path,
            'DashboardPath': dashboard_name_to_catalog_path(al.get('dashboard_name', '')),
        })
    
    return window_rows

def _synthesize_dashboard_report_views_from_reports(reports_map: Dict[str, Dict]) -> List[Dict]:
    """
    Fallback: when no dashboard pages are found, synthesize a minimal set of
    dashboard report views from standalone report XMLs so downstream CSVs are populated.
    """
    synthesized: List[Dict] = []
    # Root used to compute catalog paths from filesystem
    input_root = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'input_xml'))
    for report_path, info in reports_map.items():
        file_path = info.get('file_path', '')

        # Derive the display name from the filesystem name so we don't split on decoded '/'
        # 1) take the last FS segment (encoded), 2) convert + to space, 3) URL-decode (%2f -> '/')
        encoded_base = os.path.basename(file_path) if file_path else ''
        page_name = unquote(encoded_base.replace('+', ' ')) if encoded_base else os.path.basename(report_path.strip('/'))

        # Compute dashboard path from filesystem directory to avoid splitting on decoded '/'
        catalog_full = file_path_to_catalog_path(file_path, input_root) if file_path else report_path
        # Remove the last segment (the report itself)
        if '/' in catalog_full.strip('/'):
            dashboard_catalog = '/' + '/'.join(catalog_full.strip('/').split('/')[:-1])
        else:
            dashboard_catalog = catalog_full
        # Store without leading slash to match other callers
        dashboard_path = dashboard_catalog.strip('/')

        caption = page_name
        synthesized.append({
            'page_name': page_name,
            'dashboard_name': dashboard_path,  # e.g., shared/orderbook
            'section_name': '',
            'column_name': '',
            'display': '',
            'duid': '',
            'page_file': file_path,  # used to compute WorksheetPath
            'report_view_name': '',
            'caption': caption,
            'report_path': report_path,
            'report_type': 'report',
        })
    logger.info(f"Synthesized {len(synthesized)} dashboard-like report views from reports_map")
    return synthesized

def _synthesize_global_filters_from_prompts(prompts_map: Dict[str, Dict]) -> List[Dict]:
    """
    Fallback: synthesize global filter entries from parsed prompt XMLs so that
    Filters.csv and Windows.csv/Dashboards.csv include prompt information even without dashboards.
    """
    synthesized: List[Dict] = []
    for prompt_path, info in prompts_map.items():
        file_path = info.get('file_path', '')
        # page_name: use the prompt name (last segment)
        page_name = os.path.basename(prompt_path)
        # dashboard name/path: use the catalog folder containing the prompt
        dashboard_path = os.path.dirname(prompt_path.strip('/'))
        caption = page_name
        synthesized.append({
            'page_name': page_name,
            'dashboard_name': dashboard_path,  # e.g., shared/orderbook
            'section_name': '',
            'column_name': '',
            'duid': '',
            'page_file': file_path,
            'filter_name': page_name,
            'filter_index': 0,
            'caption': caption,
            'filter_path': prompt_path,
        })
    logger.info(f"Synthesized {len(synthesized)} global filters from prompts_map")
    return synthesized

def main() -> int:
    """
    Entry point: parse all XML files in input_xml/ and write extracted CSVs.
    """
    # CLI arguments to allow custom input/output
    parser = argparse.ArgumentParser(description='Extract dashboard/report metadata from OBIEE XML and write CSVs')
    parser.add_argument('--input', '-i', dest='input_dir', default=None,
                        help='Input directory to scan (defaults to <repo>/input_xml)')
    parser.add_argument('--output', '-o', dest='output_dir', default=None,
                        help='Output directory for CSVs (defaults to <repo>/data/tmp/output_csv)')
    args, unknown = parser.parse_known_args()

    # root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    # default_input_dir = os.path.join(root_dir, 'input_xml')
    # # Write to the orderbook-specific output folder by default
    # default_output_dir = os.path.join(root_dir, 'data', 'tmp', 'output_csv_orderbook')
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    input_xml_dir = os.path.join(root_dir, 'input_xml')
    output_dir = os.path.join(root_dir, 'data', 'tmp', 'output_csv')

    # logger.info(f"Root: {root_dir}")
    # logger.info(f"Source XML dir: {input_xml_dir}")
    # logger.info(f"Output dir: {output_dir}")

    # input_xml_dir = os.path.abspath(args.input_dir) if args.input_dir else default_input_dir
    # output_dir = os.path.abspath(args.output_dir) if args.output_dir else default_output_dir

    logger.info(f"Root: {root_dir}")
    logger.info(f"Source XML dir: {input_xml_dir}")
    logger.info(f"Output dir: {output_dir}")

    ensure_dir(output_dir)

    if not os.path.isdir(input_xml_dir):
        logger.error(f"Source directory does not exist: {input_xml_dir}")
        return 2

    # Step 1: Process all reports recursively
    logger.info("=" * 80)
    logger.info("Processing report XMLs recursively")
    logger.info("=" * 80)
    reports_map, prompts_map = process_all_reports_recursively(input_xml_dir)
    
    # Step 2: Process dashboard XMLs from input_xml directory recursively
    logger.info("=" * 80)
    logger.info("Processing dashboard XMLs recursively")
    logger.info("=" * 80)
    
    agg_dashboards, agg_dashboard_page_refs, agg_dashboard_pages, agg_dashboard_columns, \
    agg_dashboard_sections, agg_dashboard_report_views, agg_dashboard_global_filters, \
    agg_dashboard_action_links = process_all_dashboards_recursively(input_xml_dir)
    
    logger.info(f"Dashboard processing complete:")
    logger.info(f"  Dashboards: {len(agg_dashboards)}")
    logger.info(f"  Dashboard Pages: {len(agg_dashboard_pages)}")
    logger.info(f"  Report Views: {len(agg_dashboard_report_views)}")
    logger.info(f"  Global Filters: {len(agg_dashboard_global_filters)}")
    logger.info(f"  Action Links: {len(agg_dashboard_action_links)}")

    # Step 3: Create integrated CSV data
    logger.info("=" * 80)
    logger.info("Creating integrated CSV data")
    logger.info("=" * 80)
    
    # If no dashboard pages were discovered in the input scope, synthesize minimal
    # dashboard-like structures from reports/prompts so CSVs are still populated.
    if len(agg_dashboard_report_views) == 0:
        logger.info("No dashboard pages found in input scope; synthesizing rows from reports/prompts for CSV output")
        synthesized_rvs = _synthesize_dashboard_report_views_from_reports(reports_map)
        synthesized_gfs = _synthesize_global_filters_from_prompts(prompts_map)
        # Use synthesized data for CSV creation paths below
        use_rvs = synthesized_rvs
        use_gfs = synthesized_gfs
        use_als = []
    else:
        use_rvs = list(agg_dashboard_report_views)
        use_gfs = list(agg_dashboard_global_filters)
        use_als = agg_dashboard_action_links

        # Also include standalone reports/prompts not referenced by dashboards so their metadata is captured
        try:
            synthesized_rvs = _synthesize_dashboard_report_views_from_reports(reports_map)
            present_reports = {str(rv.get('report_path', '')).lower() for rv in use_rvs}
            to_add_rvs = [rv for rv in synthesized_rvs if str(rv.get('report_path', '')).lower() not in present_reports]
            if to_add_rvs:
                logger.info(f"Including {len(to_add_rvs)} standalone report views not on dashboards")
                use_rvs.extend(to_add_rvs)
        except Exception as exc:
            logger.warning(f"Failed to synthesize additional report views: {exc}")

        try:
            synthesized_gfs = _synthesize_global_filters_from_prompts(prompts_map)
            present_prompts = {str(gf.get('filter_path', '')).lower() for gf in use_gfs}
            to_add_gfs = [gf for gf in synthesized_gfs if str(gf.get('filter_path', '')).lower() not in present_prompts]
            if to_add_gfs:
                logger.info(f"Including {len(to_add_gfs)} standalone prompts not on dashboards")
                use_gfs.extend(to_add_gfs)
        except Exception as exc:
            logger.warning(f"Failed to synthesize additional prompts: {exc}")

    # Create Dashboard CSV (replaces DashboardHierarchy)
    dashboard_rows = create_dashboard_csv_data(use_rvs, use_gfs, use_als)
    
    # Create Worksheets CSV with dashboard page + report column details
    worksheet_rows = create_worksheets_csv_data(use_rvs, reports_map)
    # Post-process: remove rows where TableNames is empty or mistakenly contains SUM(...)-SUM(...) expression
    worksheet_rows, removed_tnames = _filter_erroneous_tablenames_rows(worksheet_rows)
    if removed_tnames:
        logger.info(f"Removed {removed_tnames} worksheet rows with invalid TableNames (empty or SUM(...)-SUM(...))")
    # Post-process: normalize ColumnNames OBIEE variable syntax for current month name
    worksheet_rows, replaced_cols = _normalize_columnnames_biserver_variables(worksheet_rows)
    if replaced_cols:
        logger.info(f"Replaced ColumnNames OBIEE variable syntax with plain name for {replaced_cols} rows (Rvar_Curr_MonthName)")
    
    # Create ChartType CSV
    charttype_rows = create_charttype_csv_data(use_rvs, reports_map)
    
    # Create Filters CSV
    filter_rows = create_filters_csv_data(use_rvs, use_gfs, reports_map, prompts_map)
    
    # Create Windows CSV
    window_rows = create_windows_csv_data(use_rvs, use_gfs, use_als)
    
    # Write primary CSV files
    logger.info("=" * 80)
    logger.info("Writing primary CSV files")
    logger.info("=" * 80)
    
    write_csv(os.path.join(output_dir, 'Dashboards.csv'),
              ['WorksheetName', 'DashboardName', 'ObjectName', 'ObjectType', 'ObjectPath', 'WorksheetPath', 'DashboardPath'],
              dashboard_rows)
    logger.info(f"Dashboards.csv: {len(dashboard_rows)} rows")
    
    write_csv(os.path.join(output_dir, 'Worksheets.csv'),
              ['WorksheetName', 'DashboardName', 'ReportName', 'ReportNameTag', 'ViewId',
               'DataSourceName', 'TableNames', 'ColumnNames', 'Formula', 'ColumnId', 'ColumnType', 'ExpressionType',
               'X', 'Y', 'Encoding', 'MeasureType', 'RiserType', 'Summerized_by', 'IsDerived', 'SourceColumnIds', 'SourceExpressions',
               'WorksheetPath', 'DashboardPath', 'ReportPath'],
              worksheet_rows)
    logger.info(f"Worksheets.csv: {len(worksheet_rows)} rows")
    
    write_csv(os.path.join(output_dir, 'ChartType.csv'),
              ['WorksheetName', 'DashboardName', 'ReportName', 'ReportNameTag',
               'ViewId', 'ViewType', 'ChartType', 'TitleText', 'IsCurrentView', 'Reason',
               'WorksheetPath', 'DashboardPath', 'ReportPath'],
              charttype_rows)
    logger.info(f"ChartType.csv: {len(charttype_rows)} rows")
    
    write_csv(os.path.join(output_dir, 'Filters.csv'),
              ['WorksheetName', 'DashboardName', 'ReportName',
               'FilterType', 'PromptType', 'PromptName', 'Formula', 'ColumnId', 'ColumnName', 'TableName', 
               'Direction', 'Expression', 'Operator', 'ParentOperator', 'FilterValue', 'ControlType', 
               'MaxChoices', 'IncludeAllChoices', 'Required', 'DefaultValues', 'DefaultValuesType', 
               'UsingCodeValue', 'ConstrainPromptType', 'AutoSelectValue', 'PromptVarLocation', 
               'PromptVarType', 'PromptVarFormula', 'PromptSourceType', 'PromptChoices', 'SourceFormula', 
               'Instruction', 'SubjectArea', 'Style', 'Layout', 'Position', 'Display', 
               'WorksheetPath', 'DashboardPath', 'ReportPath'],
              filter_rows)
    logger.info(f"Filters.csv: {len(filter_rows)} rows")
    
    write_csv(os.path.join(output_dir, 'Windows.csv'),
              ['WorksheetName', 'DashboardName', 'WindowName', 'WindowClass',
               'SectionName', 'ColumnName', 'Display', 'YPosition',
               'WorksheetPath', 'DashboardPath'],
              window_rows)
    logger.info(f"Windows.csv: {len(window_rows)} rows")
    
    logger.info("=" * 80)
    logger.info("All processing complete!")
    logger.info("=" * 80)

    return 0

if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as e:
        logger.error(f"Fatal error in script execution: {e}")
        sys.exit(1)