import pandas as pd
import numpy as np
from datetime import timedelta

# Generierung der Chaneg Tabellen
change_event_columns = [
    'ChangeNumber', 'ObjectID', 'ChangeDate',
    'OldValue', 'NewValue', 'ChangeType'
]
sales_order_changes = []
sales_order_item_changes = []
delivery_item_changes = []
customer_invoice_changes = []

# Abspeichern von abgebrochenen Sales Order Items
canceled_sales_order_item_ids = [] 

# Initialisiere einen globalen Zähler für ChangeNumber
change_number_counter = 1 

# Abspeichern der Return Goods Events pro Sales Order
return_goods_per_order = {}

# Definieren Sie Zeitintervalle und Ausreißer für die Ereignisgenerierung.
time_intervals = {
    'CreateSalesOrder_to_CreateSalesOrderItem': {
        'min_days': 0.5, 'max_days': 5,
        'min_hours': 8, 'max_hours': 20,
        'outlier_min_days': 0.1, 'outlier_max_days': 10
    },
    'CreateSalesOrderItem_to_GenerateDeliveryDocument': {
        'min_days': 0.3, 'max_days': 0.5,
        'min_hours': 8, 'max_hours': 20,
        'outlier_min_days': 0.1, 'outlier_max_days': 5
    },
    'GenerateDeliveryDocument_to_CreateDeliveryItem': {
        'min_days': 1, 'max_days': 3,
        'outlier_min_days': 1, 'outlier_max_days': 10
    },
    'CreateDeliveryItem_to_ReleaseDelivery': {
        'min_days': 1.5, 'max_days': 2.5,
        'outlier_min_days': 1, 'outlier_max_days': 10
    },
    'ReleaseDelivery_to_ShipGoods': {
        'min_days': 1.5, 'max_days': 2.5,
        'outlier_min_days': 1, 'outlier_max_days': 7
    },
    'ShipGoods_to_CreateInvoice': {
        'min_days': 1, 'max_days': 2,
        'outlier_min_days': 1, 'outlier_max_days': 30
    },
    'ShipGoods_to_ReceiveDeliveryConfirmation': {
        'min_days': 3, 'max_days': 12,
        'outlier_min_days': 1, 'outlier_max_days': 60
    },
    'CreateInvoice_to_SendInvoice': {
        'min_hours': 3.5, 'max_hours': 4.5,
        'outlier_min_hours': 1, 'outlier_max_hours': 48
    },
    'SendInvoice_to_ClearInvoice': {
        'min_days': 15, 'max_days': 50,
        'outlier_min_days': 30, 'outlier_max_days': 100
    },
    'ClearInvoice_to_InvoiceDueDatePassed': {
        'min_days': 5, 'max_days': 12,
        'outlier_min_days': 1, 'outlier_max_days': 24
    }
}

# Wahrscheinlichkeiten für Change Events
change_event_probabilities = {
    'ChangePrice': 0.45,
    'ChangeQuantity': 0.4,
    'ChangeConfirmedDeliveryDate': 0.35,
    'ChangeShippingCondition': 0.35,
    'SetDeliveryBlock': 0.4,
    'RemoveDeliveryBlock': 0.9, # 90% da es auf nur ausgelöst wird wenn ein SetDeliveryBlock Event stattfindet
    'SetBillingBlock': 0.25,
    'RemoveBillingBlock': 0.9, # 90% da es auf nur ausgelöst wird wenn ein SetBillingBlock Event stattfindet
    'PartialDelivery': 0.1,
    'ReturnGoods': 0.3,
    'DeliveryDatePassed': 0.15,
    'InvoiceDueDatePassed': 0.1,
    'CancelOrder': 0.15,  
    'CancelOrderAfterRG': 0.5
}

# Hilfsfunktion zur Erzeugung von Zufallszeiten auf der Grundlage einer Reihe von Stunden/Tagen
def add_random_time(base_time, min_days=0, max_days=0, min_hours=0, max_hours=0):
    random_days = np.random.uniform(min_days, max_days) if max_days > 0 else 0
    random_hours = np.random.uniform(min_hours, max_hours) if max_hours > 0 else 0
    return base_time + timedelta(days=random_days, hours=random_hours)

 
# Hilfsfunktion zur Einführung von Ausreißern auf der Grundlage einer bestimmten Wahrscheinlichkeit
def choose_time_interval(interval, base_time, outlier_probability=0.1):
    if np.random.rand() < outlier_probability:
        return add_random_time(
            base_time=base_time,
            min_days=interval.get('outlier_min_days', 0),
            max_days=interval.get('outlier_max_days', 0),
            min_hours=interval.get('outlier_min_hours', 0),
            max_hours=interval.get('outlier_max_hours', 0)
        )
    else:
        return add_random_time(
            base_time=base_time,
            min_days=interval.get('min_days', 0),
            max_days=interval.get('max_days', 0),
            min_hours=interval.get('min_hours', 0),
            max_hours=interval.get('max_hours', 0)
        )
 
# Funktion zur Entscheidung, ob ein Änderungsereignis eintritt, basierend auf der Wahrscheinlichkeit
def apply_change_event(event_type):
    return np.random.rand() < change_event_probabilities.get(event_type, 0)


# Funktion zur Verzögerung von Events nach einem Change Event
def delay_events(start_time, event_order, min_days=0, max_days=0, min_hours=0, max_hours=0):
    for event in event_order:
        new_time = add_random_time(start_time, min_days=min_days, max_days=max_days, min_hours=min_hours, max_hours=max_hours)
        # Hier stellen wir sicher, dass der Zeitstempel nur nach hinten verschoben wird
        if pd.isnull(event['timestamp']) or new_time > event['timestamp']:
            event['timestamp'] = new_time
        start_time = event['timestamp']
    return event_order

# FUnktion für die Handhabung von CancelOrder Events
def apply_cancel_order_logic(sales_order_id):
    if sales_order_id in canceled_sales_order_item_ids:
        return  # Bestellung wurde bereits storniert

    # Storniere alle zugehörigen SalesOrderItems
    related_sales_order_items = sales_order_items_df[sales_order_items_df['SalesOrderID'] == sales_order_id]
    for soi_idx in related_sales_order_items.index:
        sales_order_items_df.loc[soi_idx, ['CreateSalesOrderItem', 'GenerateDeliveryDocument']] = np.nan

        # Storniere die zugehörigen DeliveryItems
        sales_order_item_id = related_sales_order_items.loc[soi_idx, 'SalesOrderItemID']
        related_delivery_items = delivery_items_df[delivery_items_df['SalesOrderItemID'] == sales_order_item_id]
        for di_idx in related_delivery_items.index:
            delivery_items_df.loc[di_idx, ['CreateDeliveryItem', 'ReleaseDeliveryDate', 'DeliveryDate_ShipGoods', 'DeliveryDate_ReceiveConfirmation']] = np.nan

    # Storniere alle zugehörigen Rechnungen
    related_invoices = customer_invoices_df[customer_invoices_df['SalesOrderID'] == sales_order_id]
    for inv_idx in related_invoices.index:
        customer_invoices_df.loc[inv_idx, ['InvoiceDate', 'InvoiceDate_Send', 'InvoiceDate_Clear']] = np.nan

    # Markiere die Bestellung als storniert
    canceled_sales_order_item_ids.append(sales_order_id)

# Funktion, um identische Zeitstempel für Events wie ReleaseDelivery, ShipGoods, ReceiveConfirmation zu korrigieren
# und sicherzustellen, dass die Reihenfolge erhalten bleibt
def adjust_identical_timestamps_with_order_check(delivery_items_df):
    # Liste der Events in der Reihenfolge, in der sie auftreten sollen
    events = ['ReleaseDeliveryDate', 'DeliveryDate_ShipGoods', 'DeliveryDate_ReceiveConfirmation']

    # Iteriere durch alle SalesOrderItemIDs
    for sales_order_item_id in delivery_items_df['SalesOrderItemID'].unique():
        # Filter für alle DeliveryItems mit der gleichen SalesOrderItemID
        related_delivery_items = delivery_items_df[delivery_items_df['SalesOrderItemID'] == sales_order_item_id]

        # Vergleiche die Events
        for event in events:
            # Prüfe, ob alle Events denselben Zeitstempel haben
            unique_timestamps = related_delivery_items[event].nunique()
            if unique_timestamps == 1:  # Wenn alle Zeitstempel gleich sind
                # Zufällige Verzögerung zwischen 1 und 3 Tagen für jeden DeliveryItem
                for d_idx in related_delivery_items.index:
                    random_delay = timedelta(days=np.random.uniform(1, 3))
                    delivery_items_df.at[d_idx, event] = pd.to_datetime(delivery_items_df.at[d_idx, event]) + random_delay

        # Sicherstellen, dass die Reihenfolge der Events korrekt bleibt
        for d_idx in related_delivery_items.index:
            release_date = pd.to_datetime(delivery_items_df.at[d_idx, 'ReleaseDeliveryDate'])
            ship_goods_date = pd.to_datetime(delivery_items_df.at[d_idx, 'DeliveryDate_ShipGoods'])
            receive_confirmation_date = pd.to_datetime(delivery_items_df.at[d_idx, 'DeliveryDate_ReceiveConfirmation'])

            # Überprüfe und korrigiere die Reihenfolge
            if ship_goods_date <= release_date:
                ship_goods_date = release_date + timedelta(days=np.random.uniform(1, 2))
                delivery_items_df.at[d_idx, 'DeliveryDate_ShipGoods'] = ship_goods_date

            if receive_confirmation_date <= ship_goods_date:
                receive_confirmation_date = ship_goods_date + timedelta(days=np.random.uniform(3, 9))
                delivery_items_df.at[d_idx, 'DeliveryDate_ReceiveConfirmation'] = receive_confirmation_date

    return delivery_items_df

# Funktion zur Neuberechnung des TotalAmount für jede SalesOrder
def update_total_amount_all_sales_orders():
    for sales_order_id in sales_orders_df['SalesOrderID']:
        # Finde alle zugehörigen SalesOrderItems und berechne die Summe
        related_items = sales_order_items_df[sales_order_items_df['SalesOrderID'] == sales_order_id]
        new_total_amount = sum(related_items['Quantity'] * related_items['UnitPrice'])
        sales_orders_df.loc[sales_orders_df['SalesOrderID'] == sales_order_id, 'TotalAmount'] = round(new_total_amount, 2)

# Funktion zur Erstellung eines Change Events
def generate_change_event(change_type, object_id, old_value, new_value, change_date, changes_list):
    global change_number_counter 
    change_number = f"CHG{change_number_counter}"
    change_number_counter += 1  

    changes_list.append({
        'ChangeNumber': change_number,
        'ObjectID': object_id,
        'ChangeDate': change_date,
        'OldValue': old_value,
        'NewValue': new_value,
        'ChangeType': change_type
    })

# Definintion Statsiche Kunden Informationen
customer_data = {
    'CustomerID': [f'C{str(i).zfill(3)}' for i in range(1, 16)],  # 15 customers
    'CustomerName': ['Customer A', 'Customer B', 'Customer C', 'Customer D', 'Customer E',
                     'Customer F', 'Customer G', 'Customer H', 'Customer I', 'Customer J',
                     'Customer K', 'Customer L', 'Customer M', 'Customer N', 'Customer O'],
    'Location': ['New York', 'San Francisco', 'Berlin', 'London', 'Paris', 'Sydney', 'Tokyo', 'Toronto', 'Dubai', 'Rome',
                 'Mumbai', 'Moscow', 'Mexico City', 'Shanghai', 'Sao Paulo'],
    'Industry': ['Retail', 'Tech', 'Manufacturing', 'Finance', 'Retail', 'Tech', 'Tech', 'Finance', 'Retail', 'Manufacturing',
                 'Retail', 'Tech', 'Finance', 'Manufacturing', 'Retail'],
    'Region': ['North America', 'North America', 'Europe', 'Europe', 'Europe', 'Oceania', 'Asia', 'North America', 'Middle East', 'Europe',
               'Asia', 'Europe', 'North America', 'Asia', 'South America']
}

# Definition statischer Produktinformationen
product_data = {
    'ProductID': [f'P{str(i).zfill(3)}' for i in range(100, 135)],  # 35 Produkte
    'ProductName': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Monitor', 'Keyboard', 'Mouse', 'Printer', 'Router', 'Webcam',
                    'Desk', 'Office Chair', 'USB Drive', 'External Hard Drive', 'Gaming Console', 'Digital Camera', 'Bluetooth Speaker',
                    'Smartwatch', 'Drone', '4K TV', 'Smart Light Bulb', 'Power Bank', 'Projector', 'Robot Vacuum', 'Air Purifier', 
                    'Microwave', 'Washing Machine', 'Solid State Drive', 'RAM Memory', 'Motherboard', 'Custom PC', 'Smart Home System', 
                    'Electric Scooter', '3D Printer', 'Smart Fridge'],
    'UnitPrice': [1500, 900, 350, 120, 300, 50, 40, 250, 80, 60,
                  400, 150, 15, 80, 500, 700, 180, 280, 1200, 900,
                  30, 50, 350, 300, 200, 600, 800, 150, 250, 450, 1000,
                  800, 1500, 2000, 900],
    'Category': ['Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Accessories', 'Accessories', 'Electronics', 'Accessories', 'Accessories',
                 'Furniture', 'Furniture', 'Accessories', 'Accessories', 'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics',
                 'Accessories', 'Electronics', 'Home Appliances', 'Home Appliances', 'Home Appliances', 'Home Appliances', 'Home Appliances', 'Accessories', 'Electronics', 
                 'Electronics', 'Electronics', 'Electronics', 'Electronics','Electronics', 'Home Appliances'],
    'RequiresProduction': [True, False, False, False, False, False, False, False, False, False,
                           False, False, False, False, False, False, False, True, True, True,
                           False, False, True, True, False, False, False, False, True, True, 
                           True, True, True, True, True]
}

customers_df = pd.DataFrame(customer_data)
products_df = pd.DataFrame(product_data)

# Definiere die Kundengruppen basierend auf Region
group_1_customers = customers_df[customers_df['Region'].isin(['South America', 'Middle East'])]['CustomerID']
group_2_customers = customers_df[customers_df['Region'].isin(['Europe'])]['CustomerID']
group_3_customers = customers_df[customers_df['Region'].isin(['North America'])]['CustomerID']
group_4_customers = customers_df[customers_df['Region'].isin(['Asia', 'Oceania'])]['CustomerID']

# Wahrscheinlichkeiten für die Verteilung der Gruppen (z.B. höhere Wahrscheinlichkeit für Gruppe 1)
group_probabilities = [
    0.4,  # 40% der Kundenaufträge aus Gruppe 1
    0.3,  # 30% der Kundenaufträge aus Gruppe 2
    0.2,  # 20% der Kundenaufträge aus Gruppe 3
    0.1   # 10% der Kundenaufträge aus Gruppe 4
]

# Kombiniere die Kunden-IDs und die Wahrscheinlichkeiten so, dass sie die gleiche Länge haben
all_customers = (
    group_1_customers.tolist() + 
    group_2_customers.tolist() + 
    group_3_customers.tolist() + 
    group_4_customers.tolist()
)

# Passe die Wahrscheinlichkeitsliste an die Länge jeder Gruppe an
all_probabilities = (
    [group_probabilities[0] / len(group_1_customers)] * len(group_1_customers) +
    [group_probabilities[1] / len(group_2_customers)] * len(group_2_customers) +
    [group_probabilities[2] / len(group_3_customers)] * len(group_3_customers) +
    [group_probabilities[3] / len(group_4_customers)] * len(group_4_customers)
)

# Generieung der Sales Order Tabelle
num_sales_orders=3000
sales_orders_data = {
    'SalesOrderID': [f'SO{str(i).zfill(3)}' for i in range(1, num_sales_orders + 1)],
    'OrderDate': pd.date_range(start='2012-01-01', periods=num_sales_orders, freq='D'),
    'CustomerID': np.random.choice(
        all_customers,
        num_sales_orders,
        p=all_probabilities 
    ),
    'TotalAmount': np.nan
}

# DataFrame erstellen
sales_orders_df = pd.DataFrame(sales_orders_data)

# Definition von Produkten die eine hhóhe Return rate haben
high_return_products = products_df[products_df['RequiresProduction'] == True]['ProductID']
normal_products = products_df[products_df['RequiresProduction'] == False]['ProductID']

# Listen für Produkte, die Produktion erfordern, und solche, die keine Produktion benötigen
# Definiere vier Produktgruppen basierend auf Kategorie oder Produktionsanforderungen
group_1_products = products_df[products_df['Category'].isin(['Electronics']) & (products_df['RequiresProduction'] == True)]['ProductID']
group_2_products = products_df[products_df['Category'].isin(['Home Appliances']) & (products_df['RequiresProduction'] == True)]['ProductID']
group_3_products = products_df[products_df['Category'].isin(['Accessories']) & (products_df['RequiresProduction'] == False)]['ProductID']
group_4_products = products_df[products_df['Category'].isin(['Furniture', 'Electronics']) & (products_df['RequiresProduction'] == False)]['ProductID']

# Wahrscheinlichkeiten für die Verteilung der Gruppen
product_group_probabilities = [
    0.5 / len(group_1_products),  # 50% der Aufträge kommen aus Gruppe 1
    0.3 / len(group_2_products),  # 20% der Aufträge aus Gruppe 2
    0.15 / len(group_3_products),  # 20% der Aufträge aus Gruppe 3
    0.05 / len(group_4_products)   # 10% der Aufträge aus Gruppe 4
]

# Kombiniere alle Produkt IDs und Wahrscheinlichkeiten für np.random.choice
all_products = (
    group_1_products.tolist() + 
    group_2_products.tolist() + 
    group_3_products.tolist() + 
    group_4_products.tolist()
)
all_product_probabilities = (
    [product_group_probabilities[0]] * len(group_1_products) + 
    [product_group_probabilities[1]] * len(group_2_products) + 
    [product_group_probabilities[2]] * len(group_3_products) + 
    [product_group_probabilities[3]] * len(group_4_products)
)

# Production Order Tabelle erstellen
production_orders_data = []
production_order_counter = 1

# Sales Order Item Tabelle erstellen
sales_order_items_data = []
sales_order_item_counter = 1

# Generieung Sales Order Item Tabekke
sales_order_items_data = []
for sales_order_id in sales_orders_df['SalesOrderID']:
    num_items = np.random.randint(1, 5) 
    total_amount = 0  # Berechnung des TotalAmount für jede SalesOrder

    for _ in range(num_items):
        # Wähle Produkt nach Wahrscheinlichkeiten
        product_id = np.random.choice(
            all_products,
            p=all_product_probabilities
        )
        
        # Hole das Produkt basierend auf der gewählten ID
        product = products_df[products_df['ProductID'] == product_id].iloc[0]
        quantity = np.random.randint(1, 10)
        unit_price = product['UnitPrice']
        total_price = round(unit_price * quantity, 2)
        total_amount += total_price

        # Prüfe auf Produktionsanforderungen
        requires_production = False
        if product['RequiresProduction'] and np.random.rand() < 0.2:
            requires_production = True

        # Füge das SalesOrderItem hinzu
        sales_order_items_data.append({
            'SalesOrderItemID': f'SOI{sales_order_item_counter}',
            'SalesOrderID': sales_order_id,
            'ProductID': product['ProductID'],
            'Quantity': quantity,
            'UnitPrice': round(unit_price, 2),
            'CreateSalesOrderItem': np.nan,
            'GenerateDeliveryDocument': np.nan,
            'RequiresProductionOrder': requires_production
        })

        # Produktionsaufträge für Artikel mit Produktionsanforderung hinzufügen
        if requires_production:
            production_orders_data.append({
                'ProductionOrderID': production_order_counter,
                'SalesOrderItemID': f'SOI{sales_order_item_counter}',
                'CreateProductionOrder': np.nan,
                'StartProduction': np.nan,
                'EndProduction': np.nan
            })
            production_order_counter += 1

        sales_order_item_counter += 1

    # Aktualisiere den Gesamtbetrag in der SalesOrder
    sales_orders_df.loc[sales_orders_df['SalesOrderID'] == sales_order_id, 'TotalAmount'] = round(total_amount, 2)

# Erstelle DataFrame für die SalesOrderItems
sales_order_items_df = pd.DataFrame(sales_order_items_data)

# Erstelle Data Frame für die Production Orders Tabelle
production_orders_df = pd.DataFrame(production_orders_data)

# Genereiung der Delivery Items
delivery_items_data = []
delivery_item_counter = 1
for sales_order_item_id in sales_order_items_df['SalesOrderItemID']:
    num_deliveries = np.random.randint(1, 3)  # Jedes Sales Order Item kann mehrere Delivery Items haben
    for _ in range(num_deliveries):
        delivery_items_data.append({
            'DeliveryItemID': f'D{delivery_item_counter}',
            'SalesOrderItemID': sales_order_item_id,
            'CreateDeliveryItem': np.nan, 
            'ReleaseDeliveryDate': np.nan,  
            'DeliveryDate_ShipGoods': np.nan,
            'DeliveryDate_ReceiveConfirmation': np.nan,
            'TargetDeliveryDate': np.nan,  
            'ShippingCondition': np.random.choice(['Standard', 'Express']), 
            'DeliveryStatus': 'Pending'
        })
        delivery_item_counter += 1
delivery_items_df = pd.DataFrame(delivery_items_data)

# Generieung der Customer Invoices mit Verknüpfung zur SalesOrderID (eine Invoice pro SalesOrder)
customer_invoices_data = []
invoice_counter = 1

for sales_order_id in sales_orders_df['SalesOrderID']:
    customer_invoices_data.append({
        'InvoiceID': f'INV{invoice_counter}',
        'SalesOrderID': sales_order_id,  # Verweise auf SalesOrderID
        'InvoiceDate': np.nan,
        'InvoiceDate_Send': np.nan,
        'InvoiceDate_Clear': np.nan,
        'InvoiceDueDate': np.nan
    })
    invoice_counter += 1

customer_invoices_df = pd.DataFrame(customer_invoices_data)

# Statusspalten für Billing Block und Unblock zu customer_invoices_df hinzufügen
customer_invoices_df['BillingBlocked'] = False
customer_invoices_df['BillingUnblocked'] = False

# Auswahl von Kunden und Produkten für Change Events
eligible_change_event_customers = customers_df.sample(frac=0.55, random_state=42)['CustomerID']
eligible_change_event_products = products_df.sample(frac=0.5, random_state=42)['ProductID']

# Äußere Schleife über jede Sales Order
for sales_order_id in sales_orders_df['SalesOrderID']:
    
    # Innere Schleife über alle SalesOrderItems, die zur aktuellen Sales Order gehören
    related_sales_order_items = sales_order_items_df[sales_order_items_df['SalesOrderID'] == sales_order_id]
   
    # Alle zur aktuellen Sales Order in Bezeihung stehenden Customer Invoices
    related_invoices = customer_invoices_df[customer_invoices_df['SalesOrderID'] == sales_order_id]

    # Setze die Flagge für Return Goods pro Bestellung auf False
    return_goods_per_order[sales_order_id] = False

    for idx, row in related_sales_order_items.iterrows():

        # Anpassung der Wahrscheinlichkeiten und Einführung von Change Event Effekten
        def apply_change_event_with_restriction(event_type, customer_id, product_id):
            if (customer_id in eligible_change_event_customers.values) and (product_id in eligible_change_event_products.values):
                # Erhöhte Wahrscheinlichkeit für bestimmte Change Events
                if event_type in ['CancelOrder', 'ReturnGoods', 'ChangePrice', 'SetBillingBlock', 'ChangeQuantity', 'ChangeConfirmedDeliveryDate', 'PartialDelivery']:

                    return np.random.rand() < (change_event_probabilities[event_type] * 1.5)
                return np.random.rand() < change_event_probabilities[event_type]
            return False

        # Hilfsfunktion zur Verzögerung nach Change Events
        def apply_additional_delay_for_change_events(event_timestamp, change_occurred):
            if change_occurred:
                return add_random_time(event_timestamp, min_days=3, max_days=15)
            return event_timestamp
        
        # Alle zur aktuellen Sales Order Item in Beziehung stehenden Delivery Items
        related_delivery_items = delivery_items_df[delivery_items_df['SalesOrderItemID'] == row['SalesOrderItemID']]

        # Überprüfe, ob `related_delivery_items` leer ist
        if related_delivery_items.empty:
            continue

        # Abrufen des Basiszeitstempels für CreateSalesOrder
        create_sales_order_timestamp = pd.to_datetime(
            sales_orders_df.loc[sales_orders_df['SalesOrderID'] == sales_order_id, 'OrderDate'].values[0]
        )

        # Berechne den CreateSalesOrderItem Zeitstempem für jede Sales Order
        create_sales_order_item_timestamp = choose_time_interval(
            time_intervals['CreateSalesOrder_to_CreateSalesOrderItem'],
            create_sales_order_timestamp
        )
        sales_order_items_df.at[idx, 'CreateSalesOrderItem'] = create_sales_order_item_timestamp

        # Wenn eine Produktionsanforderung besteht, generiere Produktionsereignisse
        if row['RequiresProductionOrder']:
            matching_production_orders = production_orders_df[production_orders_df['SalesOrderItemID'] == row['SalesOrderItemID']]

            for prod_idx in matching_production_orders.index:
                create_production_order_timestamp = add_random_time(create_sales_order_item_timestamp, min_days=0.5, max_days=2)
                production_orders_df.at[prod_idx, 'CreateProductionOrder'] = create_production_order_timestamp

                start_production_timestamp = add_random_time(create_production_order_timestamp, min_days=1, max_days=5)
                production_orders_df.at[prod_idx, 'StartProduction'] = start_production_timestamp

                end_production_timestamp = add_random_time(start_production_timestamp, min_days=15, max_days=40)
                production_orders_df.at[prod_idx, 'EndProduction'] = end_production_timestamp

                # Setze den ReleaseDelivery-Zeitstempel für jedes DeliveryItem nach EndProduction
                for d_idx, delivery_item in related_delivery_items.iterrows():
                    release_delivery_timestamp = add_random_time(end_production_timestamp, min_days=1, max_days=3)

                    # Sicherheitsprüfung: Stelle sicher, dass der Zeitstempel nie durch einen früheren überschrieben wird
                    existing_release_delivery_timestamp = delivery_items_df.at[d_idx, 'ReleaseDeliveryDate']
                    if pd.isnull(existing_release_delivery_timestamp) or release_delivery_timestamp > existing_release_delivery_timestamp:
                        delivery_items_df.at[d_idx, 'ReleaseDeliveryDate'] = release_delivery_timestamp
                   
        # Berechne den GenerateDeliveryDocument-Zeitstempel für jedes Sales Order Item
        generate_delivery_document_timestamp = choose_time_interval(
            time_intervals['CreateSalesOrderItem_to_GenerateDeliveryDocument'],
            create_sales_order_item_timestamp
        )
        sales_order_items_df.at[idx, 'GenerateDeliveryDocument'] = generate_delivery_document_timestamp

        # Berechne CreateDeliveryItem-Zeitstempel für jedes DeliveryItem
        for d_idx, delivery_item in related_delivery_items.iterrows():
            create_delivery_item_timestamp = choose_time_interval(
                time_intervals['GenerateDeliveryDocument_to_CreateDeliveryItem'],
                generate_delivery_document_timestamp
            )
            delivery_items_df.at[d_idx, 'CreateDeliveryItem'] = create_delivery_item_timestamp

            # Berechne den ReleaseDelivery-Zeitstempel individuell für jedes DeliveryItem
            release_delivery_timestamp = choose_time_interval(
                time_intervals['CreateDeliveryItem_to_ReleaseDelivery'],
                create_delivery_item_timestamp
            )

           # Sicherheitsprüfung: Stellt sicher, dass der Zeitstempel nie durch einen früheren überschrieben wird
            existing_release_delivery_timestamp = delivery_items_df.at[d_idx, 'ReleaseDeliveryDate']
            if pd.isnull(existing_release_delivery_timestamp) or release_delivery_timestamp > existing_release_delivery_timestamp:
                delivery_items_df.at[d_idx, 'ReleaseDeliveryDate'] = release_delivery_timestamp
            else:
                release_delivery_timestamp = delivery_items_df.at[d_idx, 'ReleaseDeliveryDate']

            
            # Berechne ShipGoods-Zeitstempel individuell für jedes DeliveryItem
            ship_goods_timestamp = choose_time_interval(
                time_intervals['ReleaseDelivery_to_ShipGoods'],
                release_delivery_timestamp
            )

            # Sicherheitsprüfung: Stell sicher, dass der Zeitstempel nie durch einen früheren überschrieben wird
            existing_ship_goods_timestamp = delivery_items_df.at[d_idx, 'DeliveryDate_ShipGoods']
            if pd.isnull(existing_ship_goods_timestamp) or ship_goods_timestamp > existing_ship_goods_timestamp:
                delivery_items_df.at[d_idx, 'DeliveryDate_ShipGoods'] = ship_goods_timestamp

            # Berechne ReceiveDeliveryConfirmation-Zeitstempel individuell für jedes DeliveryItem
            receive_confirmation_timestamp = choose_time_interval(
                time_intervals['ShipGoods_to_ReceiveDeliveryConfirmation'],
                ship_goods_timestamp
            )

            # Sicherheitsprüfung: Stellt sicher, dass der Zeitstempel nie durch einen früheren überschrieben wird
            existing_receive_confirmation_timestamp = delivery_items_df.at[d_idx, 'DeliveryDate_ReceiveConfirmation']
            if pd.isnull(existing_receive_confirmation_timestamp) or receive_confirmation_timestamp > existing_receive_confirmation_timestamp:
                delivery_items_df.at[d_idx, 'DeliveryDate_ReceiveConfirmation'] = receive_confirmation_timestamp

        # Berechne den CreateInvoice Zeitstemple für jede Customer Invoice
        create_invoice_timestamp = choose_time_interval(
            time_intervals['ShipGoods_to_CreateInvoice'],
            ship_goods_timestamp
        )
        
        # Berechne den SendInvoice Zeitstempel für jede Customer Invoice
        send_invoice_timestamp = choose_time_interval(
            time_intervals['CreateInvoice_to_SendInvoice'],
            create_invoice_timestamp
        )
        
        # Sicherheitsüberprüfung: Stellt sicher, dass CreateInvoice nie nach SendInvoice liegt
        if create_invoice_timestamp >= send_invoice_timestamp:
            send_invoice_timestamp = add_random_time(
                create_invoice_timestamp, min_days=1, max_days=2
            )

        # Berechne den ClearInvoice Zeitstempel für jede Customer Invoice
        clear_invoice_timestamp = choose_time_interval(
            time_intervals['SendInvoice_to_ClearInvoice'],
            send_invoice_timestamp
        )

        #  Sicherstellung der richtigen Reihenfolge zwischen ReceiveDeliveryConfirmation und CreateInvoice
        if create_invoice_timestamp >= receive_confirmation_timestamp:
            create_invoice_timestamp = add_random_time(
                receive_confirmation_timestamp, min_days=-2, max_days=-1  
            )
            sales_order_items_df.at[idx, 'CreateInvoice_Adjusted'] = create_invoice_timestamp

        # Definition der Event Reihenfolge nach Change Events
        subsequent_events = [
            {'name': 'ReleaseDelivery', 'timestamp': release_delivery_timestamp},
            {'name': 'ShipGoods', 'timestamp': ship_goods_timestamp},
            {'name': 'CreateInvoice', 'timestamp': create_invoice_timestamp},
            {'name': 'SendInvoice', 'timestamp': send_invoice_timestamp},
            {'name': 'ReceiveDeliveryConfirmation', 'timestamp': receive_confirmation_timestamp},
            {'name': 'ClearInvoice', 'timestamp': clear_invoice_timestamp}
        ]

        # Extrahiere Produkt und Customer IDs
        sales_order_id = row['SalesOrderID']
        customer_id = sales_orders_df[sales_orders_df['SalesOrderID'] == sales_order_id]['CustomerID'].values[0]
        product_id = row['ProductID']


        # 1. ChangePrice Event
        if apply_change_event_with_restriction('ChangePrice', customer_id, product_id):
            old_price = row['UnitPrice']
            new_price = round(old_price * np.random.uniform(0.9, 1.1), 2)
            change_date = add_random_time(create_sales_order_item_timestamp, min_days=1, max_days=10)
            generate_change_event('Change Price', row['SalesOrderItemID'], old_price, new_price, change_date, sales_order_item_changes)
            sales_order_items_df.at[idx, 'UnitPrice'] = new_price
            
            # Verzögerung nachfolgender Events
            subsequent_events = delay_events(
                change_date, subsequent_events, min_days=6, max_days=15
            )  

        # 2. ChangeQuantity Event
        if apply_change_event_with_restriction('ChangeQuantity', customer_id, product_id):
            old_quantity = row['Quantity']
            new_quantity = max(1, old_quantity + np.random.randint(-2, 3))
            change_date = add_random_time(create_sales_order_item_timestamp, min_days=1, max_days=10)
            generate_change_event('Change Quantity', row['SalesOrderItemID'], old_quantity, new_quantity, change_date, sales_order_item_changes)
            sales_order_items_df.at[idx, 'Quantity'] = new_quantity
            # Verzögerung nachfolgender Events
            subsequent_events = delay_events(
                change_date, subsequent_events, min_days=6, max_days=15
            )  

        # 3. ChangeConfirmedDeliveryDate Event
        if apply_change_event_with_restriction('ChangeConfirmedDeliveryDate', customer_id, product_id):
            old_confirmed_delivery_date = release_delivery_timestamp
            new_confirmed_delivery_date = add_random_time(old_confirmed_delivery_date, min_days=5, max_days=15)
            change_date = add_random_time(create_sales_order_item_timestamp, min_days=5, max_days=15)
            generate_change_event('Change Confirmed Delivery Date', row['SalesOrderItemID'], old_confirmed_delivery_date, new_confirmed_delivery_date, change_date, sales_order_item_changes)
            row['ReleaseDeliveryDate'] = new_confirmed_delivery_date
            # Verzögerung nachfolgender Events
            subsequent_events = delay_events(
                change_date, subsequent_events, min_days=10, max_days=20
            )  
        # 4. SetBillingBlock und RemoveBillingBlock Events
        if apply_change_event_with_restriction('SetBillingBlock', customer_id, product_id):
            invoice_id = customer_invoices_df[customer_invoices_df['SalesOrderID'] == sales_order_id]['InvoiceID'].values[0]
            
            # Prüfen, ob Billing Block bereits gesetzt wurde
            if not customer_invoices_df.loc[customer_invoices_df['InvoiceID'] == invoice_id, 'BillingBlocked'].values[0]:
                change_date = add_random_time(create_sales_order_item_timestamp, min_days=3, max_days=10)
                generate_change_event('Set Billing Block', invoice_id, 'No Block', 'Billing Blocked', change_date, customer_invoice_changes)
                customer_invoices_df.loc[customer_invoices_df['InvoiceID'] == invoice_id, 'BillingBlocked'] = True  # Markieren, dass Billing Block gesetzt wurde
                
                # Verzögerung nachfolgender Events
                subsequent_events = delay_events(
                    change_date, subsequent_events, min_days=10, max_days=15
                )

                # Eventuell RemoveBillingBlock nach SetBillingBlock
                if apply_change_event_with_restriction('RemoveBillingBlock', customer_id, product_id):
                    # Prüfen, ob Billing Unblock bereits durchgeführt wurde
                    if not customer_invoices_df.loc[customer_invoices_df['InvoiceID'] == invoice_id, 'BillingUnblocked'].values[0]:
                        remove_billing_block_date = add_random_time(change_date, min_days=5, max_days=30)
                        generate_change_event('Remove Billing Block', invoice_id, 'Billing Blocked', 'No Block', remove_billing_block_date, customer_invoice_changes)
                        customer_invoices_df.loc[customer_invoices_df['InvoiceID'] == invoice_id, 'BillingUnblocked'] = True  # Markieren, dass Billing Unblock durchgeführt wurde
                        
                        # Verzögerung nachfolgender Events
                        subsequent_events = delay_events(
                            remove_billing_block_date, subsequent_events, min_days=6, max_days=10
                        )

        # 5. SetDeliveryBlock und RemoveDeliveryBlock Events
        if apply_change_event_with_restriction('SetDeliveryBlock', customer_id, product_id):
            for d_idx, delivery_item in delivery_items_df[delivery_items_df['SalesOrderItemID'] == row['SalesOrderItemID']].iterrows():
                delivery_item_id = delivery_item['DeliveryItemID']
                change_date = add_random_time(create_sales_order_item_timestamp, min_days=4, max_days=10)
                generate_change_event('Set Delivery Block', delivery_item_id, 'No Block', 'Delivery Blocked', change_date, delivery_item_changes)
                # Verzögerung nachfolgender Events
                subsequent_events = delay_events(
                    change_date, subsequent_events, min_days=6, max_days=10
                )  

                # Eventuell RemoveDeliveryBlock nach SetDeliveryBlock
                if apply_change_event_with_restriction('RemoveDeliveryBlock', customer_id, product_id):
                    remove_delivery_block_date = add_random_time(change_date, min_days=8, max_days=25)
                    generate_change_event('Remove Delivery Block', delivery_item_id, 'Delivery Blocked', 'No Block', remove_delivery_block_date, delivery_item_changes)
                    # Verzögerung nachfolgender Events
                    subsequent_events = delay_events(
                        change_date, subsequent_events, min_days=6, max_days=10
                    )  

        # Füge die Spalten zur Delivery Item Tabelle hinzu, falls sie nicht existieren
        if 'PartialDelivery' not in delivery_items_df.columns:
            delivery_items_df['PartialDelivery'] = np.nan
        if 'ReturnGoods' not in delivery_items_df.columns:
            delivery_items_df['ReturnGoods'] = np.nan

        # 6. PartialDelivery Event
        if apply_change_event_with_restriction('PartialDelivery', customer_id, product_id):
            for d_idx, delivery_item in delivery_items_df[delivery_items_df['SalesOrderItemID'] == row['SalesOrderItemID']].iterrows():
                partial_delivery_timestamp = add_random_time(delivery_item['DeliveryDate_ReceiveConfirmation'], min_days=5, max_days=10)
                generate_change_event('Partial Delivery', delivery_item['DeliveryItemID'], 'Full Delivery', 'Partial Delivery', partial_delivery_timestamp, delivery_item_changes)
                delivery_items_df.at[d_idx, 'PartialDelivery'] = partial_delivery_timestamp

        # 7. ReturnGoods Event
        if apply_change_event_with_restriction('ReturnGoods', customer_id, product_id):
            for d_idx, delivery_item in delivery_items_df[delivery_items_df['SalesOrderItemID'] == row['SalesOrderItemID']].iterrows():
                receive_confirmation_time = delivery_item['DeliveryDate_ReceiveConfirmation']
                return_goods_timestamp = add_random_time(receive_confirmation_time, min_days=50, max_days=60)
                generate_change_event('Return Goods', delivery_item['DeliveryItemID'], 'Goods Delivered', 'Goods Returned', return_goods_timestamp, delivery_item_changes)
                delivery_items_df.at[d_idx, 'ReturnGoods'] = return_goods_timestamp
                
        # 8. CancelOrder Event
        if apply_change_event_with_restriction('CancelOrder', customer_id, product_id):
            order_date_value = sales_orders_df.loc[sales_orders_df['SalesOrderID'] == sales_order_id, 'OrderDate'].values[0]
            order_date_value = pd.to_datetime(order_date_value)  # Konvertiere zu datetime, falls nötig

            # Verwende das Datum in 'add_random_time'
            cancel_order_timestamp = add_random_time(order_date_value, min_days=1, max_days=30)            
            generate_change_event('Cancel Order', sales_order_id, 'Order Active', 'Order Canceled', cancel_order_timestamp, sales_order_changes)
            apply_cancel_order_logic(sales_order_id)  # Storniert alle zugehörigen Daten für die Bestellung

        if not related_delivery_items.empty:

            delivery_items_df.at[d_idx, 'ReleaseDeliveryDate'] = release_delivery_timestamp
            delivery_items_df.at[d_idx, 'DeliveryDate_ShipGoods'] = ship_goods_timestamp
            delivery_items_df.at[d_idx, 'DeliveryDate_ReceiveConfirmation'] = receive_confirmation_timestamp

        # Aktualisierung der Zeitstempel nach allen Change Events
        release_delivery_timestamp = subsequent_events[0]['timestamp']
        ship_goods_timestamp = subsequent_events[1]['timestamp']
        receive_confirmation_timestamp = subsequent_events[2]['timestamp']
        create_invoice_timestamp = subsequent_events[3]['timestamp']
        send_invoice_timestamp = subsequent_events[4]['timestamp']
        clear_invoice_timestamp = subsequent_events[5]['timestamp']

        # Aktualisierung des Delivery Items DataFrame
        for d_idx, delivery_item in related_delivery_items.iterrows():
            delivery_items_df.at[d_idx, 'CreateDeliveryItem'] = create_delivery_item_timestamp
          
            existing_release_delivery_timestamp = delivery_items_df.at[d_idx, 'ReleaseDeliveryDate']
            if pd.isnull(existing_release_delivery_timestamp) or release_delivery_timestamp > existing_release_delivery_timestamp:
                delivery_items_df.at[d_idx, 'ReleaseDeliveryDate'] = release_delivery_timestamp

            existing_ship_goods_timestamp = delivery_items_df.at[d_idx, 'DeliveryDate_ShipGoods']
            if pd.isnull(existing_ship_goods_timestamp) or ship_goods_timestamp > existing_ship_goods_timestamp:
                delivery_items_df.at[d_idx, 'DeliveryDate_ShipGoods'] = ship_goods_timestamp
            
            existing_receive_confirmation_timestamp = delivery_items_df.at[d_idx, 'DeliveryDate_ReceiveConfirmation']
            if pd.isnull(existing_receive_confirmation_timestamp) or receive_confirmation_timestamp > existing_receive_confirmation_timestamp:
                delivery_items_df.at[d_idx, 'DeliveryDate_ReceiveConfirmation'] = receive_confirmation_timestamp

        # Aktualisierung des Customer Invoices DataFrame
        for inv_idx, invoice in related_invoices.iterrows():
            customer_invoices_df.at[inv_idx, 'InvoiceDate'] = create_invoice_timestamp
            customer_invoices_df.at[inv_idx, 'InvoiceDate_Send'] = send_invoice_timestamp
            customer_invoices_df.at[inv_idx, 'InvoiceDate_Clear'] = clear_invoice_timestamp

        # Aktualisierung des DataFrame DeliveryItems mit dem Zeitstempel ReleaseDelivery und Berechnung TargetDeliveryDates
        for d_idx, delivery_item in related_delivery_items.iterrows():

            # Berechne das TargetDeliveryDate basierend auf der Shipping COndition
            shipping_condition = delivery_items_df.at[d_idx, 'ShippingCondition']
            if shipping_condition == 'Standard':
                target_delivery_date = add_random_time(ship_goods_timestamp, min_days=10, max_days=35)
            else:  # Express shipping
                target_delivery_date = add_random_time(ship_goods_timestamp, min_days=5, max_days=15)
            
            delivery_items_df.at[d_idx, 'TargetDeliveryDate'] = target_delivery_date


        # Erstelle das Change Event DeliverDueDatePassed
        for d_idx, delivery_item in related_delivery_items.iterrows():
            target_delivery_date = pd.to_datetime(delivery_items_df.at[d_idx, 'TargetDeliveryDate'])
            receive_confirmation_time = pd.to_datetime(delivery_items_df.at[d_idx, 'DeliveryDate_ReceiveConfirmation'])

            if pd.notnull(target_delivery_date):
                if pd.isnull(receive_confirmation_time) or target_delivery_date < receive_confirmation_time:
                    delivery_due_date_passed_timestamp = add_random_time(target_delivery_date, min_days=0.5, max_days=2)
                    generate_change_event(
                        'Delivery Due Date Passed', delivery_item['DeliveryItemID'],
                        None,
                        delivery_due_date_passed_timestamp, delivery_due_date_passed_timestamp,
                        delivery_item_changes
                    )
                    delivery_items_df.at[d_idx, 'DeliveryDatePassed'] = delivery_due_date_passed_timestamp
                    
                    if pd.isnull(receive_confirmation_time):
                        receive_confirmation_time = add_random_time(delivery_due_date_passed_timestamp, min_days=1, max_days=5)
                        delivery_items_df.at[d_idx, 'DeliveryDate_ReceiveConfirmation'] = receive_confirmation_time
       

    # Aktualisierung des CustomerInvoices DataFrame mit dem InvoiceDueDate
    for inv_idx, invoice in related_invoices.iterrows():
        if pd.notnull(customer_invoices_df.at[inv_idx, 'InvoiceDate_Send']):
            invoice_due_date = add_random_time(send_invoice_timestamp, min_days=30, max_days=60) # InvoiceDueDate 30-60 Tage nach SendInvoice Zeitstempel
            customer_invoices_df.at[inv_idx, 'InvoiceDueDate'] = invoice_due_date

    # Überprüfe am Ende, ob die Zahlung nach dem Fälligkeitsdatum erfolgt ist, und füge das Event 'InvoiceDueDatePassed' hinzu
    for inv_idx, invoice in related_invoices.iterrows():
        invoice_due_date = pd.to_datetime(customer_invoices_df.at[inv_idx, 'InvoiceDueDate'])
        clear_invoice_date = pd.to_datetime(customer_invoices_df.at[inv_idx, 'InvoiceDate_Clear'])

        if pd.notnull(invoice_due_date) and pd.notnull(clear_invoice_date):
            if clear_invoice_date > invoice_due_date:
                invoice_due_date_passed_timestamp = add_random_time(invoice_due_date, min_hours=1, max_hours=3)
                generate_change_event(
                    'Invoice Due Date Passed', invoice['InvoiceID'],
                    None,
                    invoice_due_date_passed_timestamp, invoice_due_date_passed_timestamp,
                    customer_invoice_changes
                )
                customer_invoices_df.at[inv_idx, 'DueDatePassed'] = invoice_due_date_passed_timestamp


delivery_items_df = adjust_identical_timestamps_with_order_check(delivery_items_df)

# Speichere das endgültige Dataset als Excel-Datei 
final_excel_file = 'Pfad zum gewünschten Speicherort/ocpm-dataset.xlsx' # <-- Platzhalter für den Speicherort der Excel-Datei
with pd.ExcelWriter(final_excel_file) as writer:
    sales_orders_df.to_excel(writer, sheet_name='SalesOrders', index=False)
    sales_order_items_df.to_excel(writer, sheet_name='SalesOrderItems', index=False)
    delivery_items_df.to_excel(writer, sheet_name='DeliveryItems', index=False)
    customer_invoices_df.to_excel(writer, sheet_name='CustomerInvoices', index=False)
    customers_df.to_excel(writer, sheet_name='Customers', index=False) 
    products_df.to_excel(writer, sheet_name='Products', index=False)    
    production_orders_df.to_excel(writer,sheet_name='ProductionOrders', index=False)
    pd.DataFrame(
        sales_order_changes, columns=change_event_columns
    ).to_excel(writer, sheet_name='SalesOrderChanges', index=False)
    pd.DataFrame(
        sales_order_item_changes, columns=change_event_columns
    ).to_excel(writer, sheet_name='SalesOrderItemChanges', index=False)
    pd.DataFrame(
        delivery_item_changes, columns=change_event_columns
    ).to_excel(writer, sheet_name='DeliveryItemChanges', index=False)
    pd.DataFrame(
        customer_invoice_changes, columns=change_event_columns
    ).to_excel(writer, sheet_name='CustomerInvoiceChanges', index=False)

print(f"Dataset saved as {final_excel_file}")
