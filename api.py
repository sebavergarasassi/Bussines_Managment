
# visit:             https://bussinesmanagment.herokuapp.com/docs

from typing import Optional
from fastapi import FastAPI
import pandas as pd
import sqlite3
from pydantic import BaseModel
import random 
from datetime import date, timedelta

class Paymethod(BaseModel):
    Id:int
    PaymentType:str

class Products(BaseModel):
    Id:int
    Description:str
    AdquisitionCost:float

class Customers(BaseModel):
    Id:int
    CI:int
    Name:Optional[str]

class Store(BaseModel):
    Id:int
    StoreName:str
    
class Invoices(BaseModel):
    Id:int
    Date:str
    Product:int
    Quantity:int
    Customer:int
    Payment:int
    Store:int
    Tax:float=1.21
    GainMargin:float=1.35


app=FastAPI(title="Bussines Manager",description="Administrador de tiendas de negocios en Argentina",version=0.3)

def db_conection(conn_type="receive",query="None"):
    """Esta funcion nos conecta a la BD"""
    db_conexion=sqlite3.connect("stores.db")

    if conn_type=="receive":
        #Retorna un diccionario con la respuesta de la query
        pd_df=pd.read_sql(query,db_conexion)
        dict_df=pd_df.to_dict(orient="records")
        return dict_df

    if conn_type=="send":
        #Envia una query con ordenes a la BD
        cursor=db_conexion.cursor()
        cursor.execute(query)
        db_conexion.commit()
        cursor.close()

#####################################################################################################################################
#Home
#####################################################################################################################################

@app.get("/",tags=[""], include_in_schema=False)
def home():
    """Devuelve un mensaje de bienvenida al proyecto"""
    return {"Welcome to Bussines Manager, please visit /docs"}

#####################################################################################################################################
#Customers
#####################################################################################################################################

@app.get("/customers/",tags=["Customers"])
def all_costumers():
    """Esta función devuelve todos los compradores de la BD"""

    query="SELECT  * from customers"
    response=db_conection("receive",query)
    
    return response

@app.post("/customers/",tags=["Customers"])
def Insert_a_customer(customer:Customers):
    """Esta funcion crea un nuevo cliente en la BD"""

    str1=str(customer.CI)
    str2=str(customer.Name)
    query="INSERT INTO customers (CI,Name) VALUES ("+str1+",'"+str2+"')"
    db_conection("send",query)

    return {"Nuevo cliente en la BD"}

@app.put("/customers/",tags=["Customers"])
def Update_a_Customer(customer_to_update:Customers,new_customer:Customers):
    """Esta funcion modifica cliente en la BD a partir del CI (DNI)"""

    str1=str(customer_to_update.CI)
    str2=str(new_customer.CI)
    str3=str(new_customer.Name)

    query="UPDATE customers SET CI="+str2+",Name='"+str3+"' WHERE CI = "+str1      
    db_conection("send",query)

    return {"Cliente Actalizado en la BD"}

#####################################################################################################################################
#Products
#####################################################################################################################################

@app.get("/products/",tags=["Products"])
def all_productcs():
    """Esta función devuelve toda la informacion de los productos de la BD"""
   
    query="SELECT * FROM Products"
    response=db_conection("receive",query)
   
    return response

@app.post("/products/",tags=["Products"])
def Insert_new_product(product:Products):
    """Esta función inserta un producto nuevo en la BD"""

    str2=str(product.Description)
    str3=str(product.AdquisitionCost)
    str4="INSERT INTO Products (Description,AdquisitionCost) "
    str5="VALUES ('"+str2+"',"+str3+")"    
    query=str4+str5
    db_conection("send",query)

    return {"Se ha creado un nuevo producto en la BD"}

@app.put("/products/",tags=["Products"])
def Update_a_product(product_to_update:Products,new_product:Products):
    """Esta función modifica un producto en la BD a partir de un Id"""
 
    str1=str(product_to_update.Id)
    str2=str(new_product.Description)
    str3=str(new_product.AdquisitionCost)
    query="UPDATE products SET Description='"+str2+"',AdquisitionCost="+str3+" WHERE Id = "+str1
    db_conection("send",query)

    return {"Se ha modificado un producto en la BD"}

#####################################################################################################################################
#Patmethod
#####################################################################################################################################

@app.get("/paymethod/",tags=["Paymethod"])
def all_paymethod():
    """Esta función devuelve todos los medios de pago de la BD"""

    query="SELECT  * from Paymethod"
    response=db_conection("receive",query)

    return response

@app.post("/paymethod/",tags=["Paymethod"])
def Insert_a_new_paymethod(paymethod:Paymethod):
    """Esta funcion agrega un metodo de pago a la BD"""
    
    str1=str(paymethod.PaymentType)
    query="INSERT INTO Paymethod (PaymentType) VALUES ('"+str1+"')"
    db_conection("send",query)

    return {"Se agrego un nuevo medio de pago"}

#####################################################################################################################################
#Stores
#####################################################################################################################################

@app.get("/stores/",tags=["Stores"])
def all_stores():
    """Esta función devuelve TODOS los stores de la BD"""
    
    query="SELECT  Store.StoreName from store"
    response=db_conection("receive",query)

    return response

@app.post("/stores/",tags=["Stores"])
def Insert_a_store(store:Store):
    """Esta funcion agrega una Store en la BD"""

    str1=str(store.StoreName)
    query="INSERT INTO Store (StoreName) VALUES ('"+str1+"')"
    db_conection("send",query)

    return {"Se agrego una nueva Store a la BD"}

@app.put("/stores/",tags=["Stores"])
def Update_a_store(store_to_update:Store,new_store:Store):
    """Esta funcion modifica una Store en la BD a partir del nombre de store"""

    str1=store_to_update.StoreName
    str2=new_store.StoreName
    query="UPDATE store SET StoreName = '"+str2+"' WHERE StoreName = '"+str1+"' "
    db_conection("send",query)

    return {"Se actualizo una Store en la BD"}


#####################################################################################################################################
#Invoices
#####################################################################################################################################

@app.get("/invoices/all",tags=["Invoices"])
def all_invoices():
    """Esta función devuelve TODOS las facturas de la BD, con margen de ganancia incluido"""

    query="SELECT Invoices.Id as Factura_Nº, Invoices.Date as Fecha, Store.StoreName as Sucursal,Customers.Name as Comprador,"
    query+="products.Description as Detalle, Products.AdquisitionCost*Invoices.GainMargin as costo, Invoices.Quantity as cantidad," 
    query+="Invoices.Tax as Impuesto, Paymethod.PaymentType as Forma_de_pago,"
    query+="round(Products.AdquisitionCost*Invoices.Quantity*(Invoices.GainMargin),2) as factura_sin_IVA,"
    query+="round(Products.AdquisitionCost*Invoices.Quantity*(Invoices.GainMargin)*invoices.Tax,2) as factura_con_IVA "
    query+="FROM Invoices "
    query+="INNER JOIN Products on Invoices.Product=Products.Id "
    query+="INNER JOIN Customers on Invoices.Customer=Customers.Id "
    query+="INNER JOIN Paymethod on Invoices.Payment=Paymethod.Id "
    query+="INNER JOIN Store on Invoices.Store=Store.Id"
    response=db_conection("receive",query)
    
    return response

@app.get("/invoices/{store}",tags=["Invoices"])
def Total_sold_per_store(store:str):
    """Devuelve el total Facturado por sucursal(Store)"""
    query="SELECT store.StoreName as Sucursal,"
    query+="round(sum(Products.AdquisitionCost*Invoices.Quantity*(Invoices.GainMargin)),2) as Total_facturado_sin_IVA,"
    query+="round(sum(Products.AdquisitionCost*Invoices.Quantity*(Invoices.GainMargin)*invoices.Tax),2) as Total_facturado_con_IVA "
    query+="from Invoices "
    query+="inner join Products on invoices.Product=Products.Id "
    query+="inner join store on invoices.store=store.Id "
    query+="where Store.StoreName='"+store+"'"

    response=db_conection("receive",query)
    print(response[0]["Sucursal"])

    if response[0]["Sucursal"]==None:#No se encuentra la sucursal
                
        return {"No se encontro la sucursal :"+store+", verificar sucursales disponibles"}

    else:
        return response



@app.post("/invoices/",tags=["Invoices"])
def make_new_invoice(invoices:Invoices):
    """Esta función crea una nueva factura en la BD"""

    str1=str(invoices.Date)
    str2=str(invoices.Product)
    str3=str(invoices.Quantity)
    str4=str(invoices.Customer)
    str5=str(invoices.Payment)
    str6=str(invoices.Store)
    str7=str(invoices.Tax)
    str8=str(invoices.GainMargin)
    query="INSERT INTO invoices (Date,Product,Quantity,Customer,Payment,Store,Tax,GainMargin) "
    query+="VALUES ('"+str1+"',"+str2+","+str3+","+str4+","+str5+","+str6+","+str7+","+str8+")"
    db_conection("send",query)

    return {"Se creo una nueva factura en la BD"}

@app.get("/invoices/",tags=["Invoices"])
def Best_customers():
    """Esta función devuelve en orden descendente los mejores clients y beneficio percibido"""

    query="select customers.name,"
    query+="round(sum(Products.AdquisitionCost*Invoices.Quantity*Invoices.GainMargin),2) as Total_sin_iva," 
    query+="round(sum(Products.AdquisitionCost*Invoices.Quantity*Invoices.GainMargin*Invoices.Tax),2) as Total_con_iva,"
    query+="round(sum(Products.AdquisitionCost*Invoices.Quantity*(Invoices.GainMargin-1)),2) as Ganancia_por_cliente "
    query+="from Invoices "
    query+="inner join customers on customers.Id=Invoices.Customer "
    query+="inner join products on products.Id=invoices.Product "
    query+="group by customers.name "
    query+="order by total_con_iva desc"

    response=db_conection("receive",query)
    
    return response

#####################################################################################################################################
#Special
#####################################################################################################################################

def Random_date(n=1000):
    """Crea dias aleatorios para la facura"""
    today=date.today()
    random_days=random.randint(1,n)
    delta_days=timedelta(days = random_days)

    return today-delta_days
    
def Random_Id(table):
    """Devuelve un Id aleatorio dentro de los margenes de la BD, se actualiza conforme cambian los registros"""
    query='SELECT MIN('+table+'.Id),max('+table+'.Id) FROM '+table
    response=db_conection("receive",query)
    lim_inf,lim_sup=response[0]["MIN("+table+".Id)"],response[0]["max("+table+".Id)"]
    Id=random.randint(lim_inf,lim_sup)

    return Id

@app.post("/special/",tags=["Special"])
def Create_a_ramdom_invoice(invoice:Invoices):
    """Esta funcion Crea Facturas con parametros aleatorios"""
    #Random Date
    str2=str(Random_date())
    #Random Product
    str3=str(Random_Id("Products"))
    #Random Quantity
    str4=str(random.randint(1,10))
    #Random Customer
    str5=str(Random_Id("Customers"))
    #Random Payment
    str6=str(Random_Id("Paymethod"))
    #Random Store
    str7=str(Random_Id("Store"))
    str8=str(invoice.Tax)
    str9=str(invoice.GainMargin)
    
    query="INSERT INTO invoices (Date,Product,Quantity,Customer,Payment,Store,Tax,GainMargin) "
    query+="VALUES ('"+str2+"',"+str3+","+str4+","+str5+","+str6+","+str7+","+str8+","+str9+")"
    db_conection("send",query)
    
    return {"Se creado una nueva factura aleatoria en la BD"}

@app.delete("/special/",tags=["Special"])
def Delete_all_invoices():
    """Esta funcion borra todas las facturas"""

    query="DELETE from invoices "
    db_conection("send",query)
    
    return {"Se borraron todas las facturas de la BD"}

