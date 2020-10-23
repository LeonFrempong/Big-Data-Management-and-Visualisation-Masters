import pandas as pd

data=pd.read_csv("C:\\Users\\LeonFremz\\PycharmProjects\\cleanDataExample\\dataset.csv",encoding='latin1')

#Reference 1 : https://www.youtube.com/watch?v=ZOX18HfLHGQ&t=213s
#Reference 2 : https://www.kaggle.com/coni57/customer-segmentation

                         #displays dataset
print(data.shape)
pd.set_option('display.max_columns',None)
pd.set_option('display.expand_frame_repr',False)
print(data)

              #finds out how many missing values in each column

print(data.isnull().sum().sum())
print(data.isnull().sum())
data.info()

                           #fill in missing data
print(data['CustomerID'])
median = data['CustomerID'].median()
print(median)
data['CustomerID'].fillna(median, inplace=True)
print(data['CustomerID'])

                 #Remvoing the letter "C" from invoiceNo column
data["Removed"] = data["InvoiceNo"].str.startswith("C")
data["Removed"] = data["Removed"].fillna(False)
#Handling incorrect Description
data = data[data["Description"].str.startswith("?")==False]
data = data[data["Description"].str.isupper() == True]
data = data[data["Description"].str.contains("LOST") == False]
data = data[data["CustomerID"].notnull()]
data["CustomerID"] = data["CustomerID"].astype(int)
# Convert Invoice Number to integer
data['InvoiceNo'].replace(to_replace="\D+", value=r"", regex=True, inplace=True)
data['InvoiceNo'] = data['InvoiceNo'].astype('int')

# remove shipping invoices
data = data[(data["StockCode"] != "DOT") & (data["StockCode"] != "POST")]
data.drop("StockCode", inplace=True, axis=1)

# convert date to proper datetime
data["InvoiceDate"] = pd.to_datetime(data["InvoiceDate"])
data.info()
print(data)

#data.to_csv(r'C:\Users\LeonFremz\Documents\datasetFixed.csv', index=False, header=True)




