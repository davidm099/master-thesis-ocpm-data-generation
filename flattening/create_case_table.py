import pandas as pd

# Laden der relevanten Tabellen
file_path = 'Pfad zum OCPM Datensatz' # <-- Platzhalter für den Speicherort des OCPM Datensatzes
excel_data = pd.ExcelFile(file_path)

# Einlesen der Tabellen
sales_orders_df = excel_data.parse('SalesOrders')
sales_order_items_df = excel_data.parse('SalesOrderItems')
delivery_items_df = excel_data.parse('DeliveryItems')

# 1. Erstellen der Grundstruktur der Case-Tabelle mit TotalAmount
case_table = sales_orders_df[['SalesOrderID', 'CustomerID', 'TotalAmount']].merge(
    sales_order_items_df[['SalesOrderID', 'SalesOrderItemID', 'ProductID']],
    on='SalesOrderID',
    how='inner'
)

# Spaltennamen anpassen
case_table = case_table.rename(columns={
    'SalesOrderItemID': 'CaseKey',
    'CustomerID': 'Customer',
    'ProductID': 'Product',
    'TotalAmount': 'TotalAmount'
})

# 2. Hinzufügen der ShippingCondition aus DeliveryItems basierend auf SalesOrderItemID
first_shipping_condition = delivery_items_df.groupby('SalesOrderItemID').first().reset_index()[['SalesOrderItemID', 'ShippingCondition']]

# Führt den Join aus, um die erste ShippingCondition hinzuzufügen
case_table = case_table.merge(
    first_shipping_condition,
    left_on='CaseKey',
    right_on='SalesOrderItemID',
    how='left'
).drop(columns=['SalesOrderItemID'])

# 3. Berechnung des Attributs DeliveryDatePassed
# Standardmäßig auf 0 setzen (on time)
case_table['DeliveryDatePassed'] = 0

# Identifiziere verspätete Lieferungen
late_deliveries = delivery_items_df[delivery_items_df['DeliveryDate_ReceiveConfirmation'] > delivery_items_df['TargetDeliveryDate']]

# Setze DeliveryDatePassed auf 1, wenn verspätet
case_table['DeliveryDatePassed'] = case_table['CaseKey'].apply(
    lambda case_key: 1 if case_key in late_deliveries['SalesOrderItemID'].values else 0
)

# OSpeichern der Case-Tabelle als Datei
case_table.to_csv('Pfad zum gewünschten Speicherort/case-table.csv', index=False)  # <-- Platzhalter für den Speicherort der CSV-Datei
