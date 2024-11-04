import pandas as pd

# Lade den Datensatz
new_file_path = 'Pfad zum OCPM Datensatz' # <-- Platzhalter für den Speicherort des OCPM Datensatzes
new_excel_data = pd.ExcelFile(new_file_path)

# Einlesen der relevanten Tabellen
sales_orders_df = new_excel_data.parse('SalesOrders')
sales_order_items_df = new_excel_data.parse('SalesOrderItems')
delivery_items_df = new_excel_data.parse('DeliveryItems')
customer_invoices_df = new_excel_data.parse('CustomerInvoices')
production_orders_df = new_excel_data.parse('ProductionOrders')
sales_order_changes_df = new_excel_data.parse('SalesOrderChanges')
sales_order_item_changes_df = new_excel_data.parse('SalesOrderItemChanges')
delivery_item_changes_df = new_excel_data.parse('DeliveryItemChanges')
customer_invoice_changes_df = new_excel_data.parse('CustomerInvoiceChanges')

# Initialisiere eine Liste für den Event Log
event_log_rows = []

# Funktion zum Extrahieren von Events aus Tabellen
def extract_events_from_table(df, case_id_col, events):
    for _, row in df.iterrows():
        case_id = row[case_id_col]
        for event, timestamp in events.items():
            if pd.notna(row[timestamp]):
                event_log_rows.append({
                    "CaseKey": case_id,
                    "Activity": event,
                    "Timestamp": row[timestamp]
                })

# Funktion zur Aggregation von Events basierend auf frühester/spätester Vorkommen
def aggregate_events(df, case_key_col, events, aggregation="earliest"):
    for case_key in df[case_key_col].unique():
        subset = df[df[case_key_col] == case_key]
        for event, timestamp_col in events.items():
            if aggregation == "earliest":
                event_timestamp = subset[timestamp_col].min()
            else:
                event_timestamp = subset[timestamp_col].max()
            if pd.notna(event_timestamp):
                event_log_rows.append({
                    "CaseKey": case_key,
                    "Activity": event,
                    "Timestamp": event_timestamp
                })

# Funktion zum Hinzufügen einzigartiger Change-Events
def add_unique_change_events(df, case_id_col, object_id_col, change_type_col, change_date_col):
    for _, row in df.iterrows():
        case_key = row[case_id_col]
        event_log_rows.append({
            "CaseKey": case_key,
            "Activity": row[change_type_col],
            "Timestamp": row[change_date_col]
        })

# Funktion zum Hinzufügen aller Change-Events
def add_all_change_events():
    # SalesOrder Changes
    add_unique_change_events(sales_order_changes_df.merge(
        sales_order_items_df[['SalesOrderID', 'SalesOrderItemID']], 
        left_on='ObjectID', right_on='SalesOrderID'), 
        'SalesOrderItemID', 'ObjectID', 'ChangeType', 'ChangeDate'
    )

    # SalesOrderItem Changes
    add_unique_change_events(sales_order_item_changes_df, 'ObjectID', 'ObjectID', 'ChangeType', 'ChangeDate')
    # Erstelle eine Zuordnung zwischen CustomerInvoice und SalesOrderItem über SalesOrderID
    customer_invoices_with_items = customer_invoices_df.merge(
        sales_order_items_df[['SalesOrderID', 'SalesOrderItemID']],
        on='SalesOrderID',
        how='inner'
    )

    # Überprüfe, ob CustomerInvoiceChanges die Spalten ObjectID und ChangeType enthält
    required_columns_customer_invoice_changes = ['ObjectID', 'ChangeType', 'ChangeDate']
    missing_columns = [col for col in required_columns_customer_invoice_changes if col not in customer_invoice_changes_df.columns]

    if missing_columns:
        raise ValueError(f"Fehlende Spalten in der CustomerInvoiceChanges-Tabelle: {missing_columns}")

    # Join CustomerInvoiceChanges mit der Zuordnung CustomerInvoices_with_Items, um SalesOrderItemID zu erhalten
    customer_invoices_changes_linked = customer_invoice_changes_df.merge(
        customer_invoices_with_items[['InvoiceID', 'SalesOrderItemID']].rename(columns={'InvoiceID': 'ObjectID'}),
        on='ObjectID',
        how='inner'
    )

    # Füge die Change Events hinzu
    add_unique_change_events(customer_invoices_changes_linked, 'SalesOrderItemID', 'ObjectID', 'ChangeType', 'ChangeDate')


    # DeliveryItem Changes
    for _, item_row in sales_order_items_df.iterrows():
        sales_order_item_id = item_row['SalesOrderItemID']
        delivery_items_for_item = delivery_items_df[delivery_items_df['SalesOrderItemID'] == sales_order_item_id]
        for _, delivery_row in delivery_items_for_item.iterrows():
            delivery_item_id = delivery_row['DeliveryItemID']
            unique_changes = delivery_item_changes_df[(delivery_item_changes_df['ObjectID'] == delivery_item_id) & 
                                                      (delivery_item_changes_df['ChangeType'].isin([
                                                          'Return Goods', 'Partial Delivery', 
                                                          'Delivery Due Date Passed', 
                                                          'Set Delivery Block', 'Remove Delivery Block']))] \
                                                      .drop_duplicates(subset=['ChangeType'])
            for _, change_row in unique_changes.iterrows():
                event_log_rows.append({
                    "CaseKey": sales_order_item_id,
                    "Activity": change_row['ChangeType'],
                    "Timestamp": change_row['ChangeDate']
                })

# Primäre Events extrahieren und aggregieren
extract_events_from_table(sales_order_items_df, 'SalesOrderItemID', {
    "CreateSalesOrderItem": 'CreateSalesOrderItem',
    "GenerateDeliveryDocument": 'GenerateDeliveryDocument',
    "CreateInvoice_Adjusted": 'CreateInvoice_Adjusted'
})

aggregate_events(delivery_items_df, 'SalesOrderItemID', {
    "CreateDeliveryItem": 'CreateDeliveryItem',
    "ReleaseDelivery": 'ReleaseDeliveryDate',
    "ShipGoods": 'DeliveryDate_ShipGoods',
    "ReceiveConfirmation": 'DeliveryDate_ReceiveConfirmation'
}, aggregation="earliest")

# CustomerInvoices Events
unique_customer_invoices = customer_invoices_df.merge(
    sales_order_items_df[['SalesOrderID', 'SalesOrderItemID']], 
    on='SalesOrderID', how='inner').drop_duplicates(subset=['SalesOrderItemID'])
extract_events_from_table(unique_customer_invoices, 'SalesOrderItemID', {
    "InvoiceCreated": 'InvoiceDate',
    "SendInvoice": 'InvoiceDate_Send',
    "ClearInvoice": 'InvoiceDate_Clear',
    "DueDatePassed": 'DueDatePassed'
})

# ProductionOrders Events
extract_events_from_table(production_orders_df, 'SalesOrderItemID', {
    "CreateProductionOrder": 'CreateProductionOrder',
    "StartProduction": 'StartProduction',
    "EndProduction": 'EndProduction'
})

# OrderDate hinzufügen
sales_order_with_items = sales_orders_df.merge(
    sales_order_items_df[['SalesOrderID', 'SalesOrderItemID']], 
    on='SalesOrderID', how='inner')
for _, row in sales_order_with_items.iterrows():
    event_log_rows.append({
        "CaseKey": row['SalesOrderItemID'],
        "Activity": "OrderDate",
        "Timestamp": row['OrderDate']
    })

# Hinzufügen aller Change-Events
add_all_change_events()

# Deduplizieren von Invoice und Delivery Change Events
final_deduplicated_event_log_rows = []
invoice_event_tracker = set()
delivery_change_event_tracker = set()

for event in event_log_rows:
    case_key = event['CaseKey']
    activity = event['Activity']
    if activity in ["InvoiceCreated", "SendInvoice", "ClearInvoice", "DueDatePassed"]:
        unique_invoice_event = (case_key, activity)
        if unique_invoice_event not in invoice_event_tracker:
            invoice_event_tracker.add(unique_invoice_event)
            final_deduplicated_event_log_rows.append(event)
    elif activity in ["Partial Delivery", "Return Goods", "Delivery Due Date Passed"]:
        unique_delivery_event = (case_key, activity)
        if unique_delivery_event not in delivery_change_event_tracker:
            delivery_change_event_tracker.add(unique_delivery_event)
            final_deduplicated_event_log_rows.append(event)
    else:
        final_deduplicated_event_log_rows.append(event)

# Den finalen Event Log als DataFrame speichern
final_complete_event_log_df = pd.DataFrame(final_deduplicated_event_log_rows).sort_values(by=['CaseKey', 'Timestamp']).reset_index(drop=True)

# Datei speichern (kann als CSV oder Excel exportiert werden)
final_complete_event_log_df.to_csv('Pfad zum gewünschten Speicherort/event-log.csv', index=False)  # <-- Platzhalter für den Speicherort der CSV-Datei
