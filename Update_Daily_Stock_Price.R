library(cronR)
library(rvest)
library(dplyr)
library(tidyr)
library(XML)
library(RMySQL)

# Daily Stock prices and Indexes from TWSE (Daily updated)
Index_Category <- c('價格指數(臺灣證券交易所)', '價格指數(跨市場)', '價格指數(臺灣指數公司)',
                    '報酬指數(臺灣證券交易所)', '報酬指數(跨市場)', '報酬指數(臺灣指數公司)')
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


Today_Prices_TWSE <- as.data.frame(Today_Prices_table[9])
Today_Prices_TWSE <- Today_Prices_TWSE[-(1:2),]
colnames(Today_Prices_TWSE) <- c('Id', 'Name', 'Volume', 'Deals', 'Amounts', 'Open', 'High', 'Low', 'Close', 'Direction', 'Diff', 
  'Last_Buy_Price', 'Last_Buy_Volume', 'Last_Sell_Price', 'Last_Sell_Volume', 'P/E ratio')
Today_Prices_TWSE$Date <- Sys.Date()

# Daily Institutional Investors' Trading of Each Stock (Daily updated)
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
  Institutional_Investor_Trading$Date <- as.Date(as.character(date), format="%Y%m%d")
  Institutional_Investor_Trading <- Institutional_Investor_Trading[which((Institutional_Investor_Trading$Institutional_Investors_Net != '')),]
  return(data.frame(Institutional_Investor_Trading))
}

Today_Institutional_Investor_Trading <- Daily_Institutional_Investor_Trading(format(Sys.Date(), '%Y%m%d'))

StocksDB <- dbConnect(MySQL(), dbname = "Stocks(TW)", username="root", password="") # 建立資料庫連線
RMySQL::dbWriteTable(StocksDB, 'History_Index_TWSE', value = Today_Index, overwrite = F, append = T,row.names = FALSE)
RMySQL::dbWriteTable(StocksDB, 'History_Prices_TWSE', value = Today_Prices_TWSE, overwrite = F, append = T,row.names = FALSE)
RMySQL::dbWriteTable(StocksDB, 'Institutional_Investor_Trading_TWSE', value = Today_Institutional_Investor_Trading, overwrite = F, row.names = FALSE, append = T)

dbDisconnect(StocksDB)


