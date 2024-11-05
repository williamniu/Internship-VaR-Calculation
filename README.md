# Internship_VaR-Calculation
## Project Description
Automatically update the yield of fund investment products and calculate VaR, identify the differences between the existing position database and the products in the yield series to be updated and import the latest data from Wind Financial Data Terminal.
## Implementation
1、Categorize investment products such as equities and funds into domestic and offshore and update the daily rate of return      
2、Determine if all stocks and fund products are in the position database     
3、For late-listed stocks, the return of the Shenwan Industry Index for the industry to which the company belongs is used as a proxy       
4、Find the worst 1% performance of a given account on a specific asset  (a negative value indicates a loss situation) and calculate and store it based on the past year's data and the past three years' data, respectively     



