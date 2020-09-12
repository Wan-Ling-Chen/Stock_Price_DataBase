library(jsonlite)
library(dplyr)
library(rvest)
library(tidyr)
library(XML)
library(RCurl)
library(RMySQL)

## Create the stock list and category (Table name: Stocks_Categories)
Stock <- read_html('https://www.cmoney.tw/follow/channel/category-stock')
StockCat <- html_nodes(Stock, 'section')


StockList <- NULL
for( i in 1 : length(StockCat)){
  Category <- gsub('[\t ]','',html_text(StockCat[[i]]))
  Category <- as.data.frame(strsplit(Category, split = '\r\n'))
  colnames(Category) <- 'SubCategory'
  Category <- subset(Category, SubCategory != '')
  Category$MainCategory <- Category[1,]
  Category <- Category[-1,]
  StockList <- rbind(StockList, Category)
}

# get links of each category to find out the each stock item
StockList$Link <- StockCat %>% html_nodes('a') %>% html_attr('href')
StockList$Link <- paste('https://www.cmoney.tw',StockList$Link, sep ='')

Stocks <- NULL
for (i in 1: nrow(StockList)){
  Stock <- read_html(StockList[i,'Link'])
  Stock <- as.data.frame(html_table(Stock)[[2]]['個股名稱'])
  Stock$MainCategory <- StockList[i, 'MainCategory']
  Stock$SubCategory <- StockList[i, 'SubCategory']
  Stocks <- rbind(Stocks, Stock)
  Sys.sleep(3)
}

Stocks$Id <- sapply(strsplit(Stocks[,'個股名稱'], split = ' ()'), function(x) x[2])
Stocks$Name <- sapply(strsplit(Stocks[,'個股名稱'], split = ' ()'), function(x) x[1])
Stocks <- Stocks[,-which(colnames(Stocks) == '個股名稱')]
Stocks$Id <- gsub('[()]','' ,Stocks$Id ) 
write.csv(Stocks, 'Stocks_Categories.csv', row.names = F, fileEncoding = "utf8")


## Historical Stocks Prices (via Yahoo Finance Api. Updated on every 11th.) (Table name: History_Prices)

# Create a function to get the json file of historical prices of each stock from yahoo finance API and sort the data 
HistoryStockPrice <- function(StartDate = 0, EndDate, StockId){
  url <- paste('https://query1.finance.yahoo.com/v8/finance/chart/',StockId,'.TW?period1=',StartDate,'&period2=', EndDate,'&interval=1d&events=history&=hP2rOschxO0', sep='')
  StockJSON <- fromJSON(url)
  StocksDate <- data.frame(as.Date(as.POSIXct(unlist(StockJSON[["chart"]][["result"]][["timestamp"]]), origin = "1970-01-01")))
  StocksPrice <- matrix(unlist(StockJSON[["chart"]][["result"]][["indicators"]][["quote"]]), nrow(StocksDate))
  StocksAdjClose <- data.frame(unlist(StockJSON[["chart"]][["result"]][["indicators"]][["adjclose"]]))
  StocksPrices <- cbind(StocksDate, StocksPrice, StocksAdjClose)
  colnames(StocksPrices) <- c('Date', 'Open', 'Close', 'High', 'Volume', 'Low', 'AdjClose')
  StocksPrices$Id <- StockId
  rownames(StocksPrices) <- NULL
  return(StocksPrices)
}

# Apply the function to each stock with the stock list which we created above 
HistoryStockPrices <- NULL
Now <- as.integer(Sys.time())

for (i in 1:nrow(Stocks)){
  tryCatch({
    HistoryData <- HistoryStockPrice(0, Now,Stocks[i,'Id'])
    HistoryStockPrices <- rbind(HistoryStockPrices, HistoryData)
    print(i)
    Sys.sleep(3)
  }, error=function(e){print('error')})
}  
saveRDS(HistoryStockPrices,'HistoryStockPrice_20200823.rds')


## Download Monthly Revenue Report of Stocks from TWSE site (Updated on every 11th) (Table name: Monthly_Revenue)

# Create a series of years and months
month <-c()
for (i in 102:109){
  for (j in 1:12){
    year_month <- paste(i, j, sep ='_')
    month <- append(month, year_month)
  }
}

# Create a function to download the monthly revenue report
download_monthly_revenue <- function(month){
  csv_url <- paste('https://mops.twse.com.tw/nas/t21/sii/t21sc03_', month , '.csv', sep = '')
  filepath <- paste('MonthlyRevenue/', month, '.csv', sep = '')
  download.file(csv_url, filepath)
}

# Download all historical monthly revenue reports
sapply(month, download_monthly_revenue)
filepath <- paste('MonthlyRevenue/', month, '.csv', sep = '')
MonthlyRevenue <- do.call(rbind, lapply(filepath, read.csv))
MonthlyRevenue <- MonthlyRevenue[,-1]


# Read all csv files of all monthly revenue reports
SplitDate <- do.call(rbind,strsplit(MonthlyRevenue$資料年月, split = '/'))
MonthlyRevenue$資料年月 <- as.character(paste0((as.numeric(SplitDate[,1])+1911), 
                                           ifelse(as.numeric(SplitDate[,2]) < 10 ,paste0('0',SplitDate[,2], sep =''), SplitDate[,2]), sep = ''))
colnames(MonthlyRevenue) <- c('Date', 'Id', 'Name', 'Industry', 'Monthly_Revenue', 'Last_Monthly_Revenue','Last_Year_Monthly_Revenue', 
                              'MOM', 'YOY', 'Cumsum_Yearly_Revenue', 'Cumsum_Last_Yearly_Revenue', 'Cumsum_YOY', 'Note')



## Daily indexes close from TWSE site (Daily updated) (Table name: History_Index_TWSE)

# List out all the categories of Indexes
Index_Category <- c('價格指數(臺灣證券交易所)', '價格指數(跨市場)', '價格指數(臺灣指數公司)',
                    '報酬指數(臺灣證券交易所)', '報酬指數(跨市場)', '報酬指數(臺灣指數公司)')

# Extract the tables from the TWSE site
url <- 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&type=ALLBUT0999'
Today_Prices <- read_html(url)
Today_Prices_table <- html_table(Today_Prices)
Today_Index <- NULL

for (i in 1:6){
  Index_Table <- as.data.frame(Today_Prices_table[i])
  colnames(Index_Table) <- c('Index', 'Close', 'Direction', 'Diff', 'Diff(%)', 'Note')
  Index_Table <- Index_Table[-1,]
  Index_Table$Date <- Sys.Date()
  Index_Table$Category <- Index_Category[i]
  Today_Index <- rbind(Today_Index, Index_Table)
}

## Daily stock prices from TWSE site (Daily updated) (Table name: History_Prices_TWSE)

Today_Prices_TWSE <- as.data.frame(Today_Prices_table[9])
Today_Prices_TWSE <- Today_Prices_TWSE[-(1:2),]
colnames(Today_Prices_TWSE) <- c('Id', 'Name', 'Volume', 'Deals', 'Amounts', 'Open', 'High', 'Low', 'Close', 'Direction', 'Diff', 
                                 'Last_Buy_Price', 'Last_Buy_Volume', 'Last_Sell_Price', 'Last_Sell_Volume', 'P/E ratio')
Today_Prices_TWSE$Date <- Sys.Date()


# Daily institutional investors' Trading of Each Stock (Daily updated) (Table name: Institutional_Investor_Trading_TWSE)

# Create the function to download the institutional investors' Trading records
Daily_Institutional_Investor_Trading <- function(date){
  csv_url <- paste('http://www.tse.com.tw/fund/T86?response=csv&date=', date , '&selectType=ALLBUT0999', sep = '')
  filepath <- paste('Daily_Institutional_Investor_Trading/', date, '.csv', sep = '')
  download.file(csv_url, filepath)
  Institutional_Investor_Trading <- readLines(filepath, skipNul = F)
  Institutional_Investor_Trading <- read.table(text = Institutional_Investor_Trading, sep = ",", skip = 1, fill = TRUE)
  Institutional_Investor_Trading <- Institutional_Investor_Trading[,-c(2,20)] 
  Institutional_Investor_Trading <- Institutional_Investor_Trading[-1,]
  colnames(Institutional_Investor_Trading) <- c('Id', 'Chinese_Investors_Net_Buy', 'Chinese_Investors_Net_Sell', 'Chinese_Investors_Net',
                                                'Foreign_Investors_Net_Buy', 'Foreign_Investors_Net_Sell', 'Foreign_Investors_Net','Investment_Trust_Net_Buy', 'Investment_Trust_Net_Sell', 'Investment_Trust_Net',
                                                'Dealers_Net', 'Dealers_Net_Buy(Self-Manages)', 'Dearlers_Net_Sell(Self-Manages)', 'Dearlers_Net(Self-Managed)',
                                                'Dealers_Net_Buy(Hedging)', 'Dearlers_Net_Sell(Hedging)', 'Dearlers_Net(Hedging)', 'Institutional_Investors_Net')
  Institutional_Investor_Trading$Id <- gsub('=','', Institutional_Investor_Trading$Id )
  Institutional_Investor_Trading$Date <- as.Date(as.character(date), format="%Y%M%d")
  Institutional_Investor_Trading <- Institutional_Investor_Trading[which((Institutional_Investor_Trading$Institutional_Investors_Net != '')),]
  return(data.frame(Institutional_Investor_Trading))
}

Today_Institutional_Investor_Trading <- Daily_Institutional_Investor_Trading(20200821)


## Write all data into MySQL 
StocksDB <- dbConnect(MySQL(), dbname = "Stocks(TW)", username="root", password="") # 建立資料庫連線

dbListTables(StocksDB)
RMySQL::dbWriteTable(StocksDB, 'Stocks_Categories', value = Stocks, overwrite = T,row.names = FALSE)
RMySQL::dbWriteTable(StocksDB, 'History_Prices', value = HistoryStockPrice_20200823, overwrite = T,row.names = FALSE)
RMySQL::dbWriteTable(StocksDB, 'Monthly_Revenue', value = MonthlyRevenue, overwrite = T, row.names = FALSE)

RMySQL::dbWriteTable(StocksDB, 'History_Index_TWSE', value = Today_Index, overwrite = T, row.names = FALSE)
RMySQL::dbWriteTable(StocksDB, 'History_Prices_TWSE', value = Today_Prices_TWSE, overwrite = T, row.names = FALSE)
RMySQL::dbWriteTable(StocksDB, 'Institutional_Investor_Trading_TWSE', value = Today_Institutional_Investor_Trading, overwrite = T, row.names = FALSE)
dbDisconnect(StocksDB)
