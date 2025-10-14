import os
from newsapi import NewsApiClient
from langchain.schema.runnable import RunnablePassthrough, RunnableMap
from langchain.schema.output_parser import StrOutputParser
from langchain.prompts import PromptTemplate
from langchain_ollama import OllamaLLM
from langchain.chains import SequentialChain
import pandas as pd
import numpy as np
import yfinance as yf
import asyncio
from concurrent.futures import ThreadPoolExecutor
from transformers import pipeline

#"9b3120bcf0cb4f0f9600e6a18b31f7fb"
companies = {"AAPL","GOOGL","META"}
def get_news():
    key = os.getenv("NEWSAPI_KEY")
    nap = NewsApiClient(api_key = key)
    news = []
    for company in companies:
        info = nap.get_everything(
            q=company,
            language = 'en',
            sort_by = 'publishedAt'
        )
        news.append(process_info(info["articles"],company))
    return pd.concat(news, ignore_index=True)

def get_stocks():
    stock_info = yf.download(companies, period ="5d",interval ="1d")
    stock_info = stock_info.reset_index()
    stock_info.columns = ['_'.join(col).strip() for col in stock_info.columns.values]
    stock_info.rename(columns={"Date_":"date"},inplace=True)
    stock_info["date"] = pd.to_datetime(stock_info["date"]).dt.date
    stock_info = stock_info.melt(id_vars="date",var_name="ticker_metric",value_name="value")
    stock_info[["metric","ticker"]] = stock_info["ticker_metric"].str.split("_",expand = True)
    stock_info.drop(columns=["ticker_metric"], inplace=True)
    stock_info = stock_info.pivot(index = ["date","ticker"], columns="metric",values="value").reset_index()
    return stock_info

def process_info(news, company):
    df = pd.DataFrame([{
        "title": article.get("title",""),
        "description": article.get("description",""),
        "content": article.get("content",""),
        "source": article.get("source",""),
        "publishedAt": article.get("publishedAt",""),
        "url": article.get("url",""),
        "ticker": company
    }for article in news])
    keys = ["title","description","content","publishedAt","url"]
    df = df.drop_duplicates(subset=keys,keep="first")
    df["text"] = df["title"] + ", " + df["description"] + ", " + df["content"]
    dat = pd.to_datetime(df["publishedAt"])
    df["date"] =  dat.dt.date
    df.drop(["title","description","content","publishedAt"], axis = 1, inplace=True)
    df = df.fillna("")
    return df

def sentiment_analysis(newsdf):
    setiment_pipe = pipeline("sentiment-analysis", model = "ProsusAI/finbert")
    pipe = setiment_pipe(newsdf['text'].tolist(), batch_size = 32)
    newsdf['sentiment'] = [i["score"] for i in pipe]
    return newsdf

async def merge_news_stocks(newsdf,stockdf):
    valid_dates = set(stockdf['date'])
    def adjust_when_closed(date):
        while date not in valid_dates and date>min(valid_dates):
            date-=pd.Timedelta(days=1)
        return date
    newsdf['date'] = newsdf['date'].apply(adjust_when_closed)
    merged = pd.merge(newsdf,stockdf,on=["date","ticker"], how = 'left')
    merged = merged.fillna(0)
    merged.drop(["source","url","date","text"], axis = 1, inplace=True)
    merged = merged.groupby("ticker", as_index= False).mean(numeric_only = True)
    return merged

def make_correlation_chain(model):
    prompt = PromptTemplate.from_template("""
Analyze correlations between sentiment and stock price.
Data:
{context}
Return JSON with correlation per ticker.
""")
    return prompt | model | StrOutputParser()

def make_impact_chain(model):
    prompt = PromptTemplate.from_template("""
Predict likely short-term price impact for each ticker based on:
{correlation_output}
Context:
{context}
Return concise summary per company.
""")
    return prompt | model | StrOutputParser()

def run_model(mergeddf):
    model = OllamaLLM(model = "llama3")
    sequence = RunnableMap({
        "correlation_output": make_correlation_chain(model),
        "context": RunnablePassthrough()
    }) | make_impact_chain(model)
    return sequence.invoke({"context": mergeddf.to_string(index=False)})

async def get_data():
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        news = loop.run_in_executor(executor,get_news)
        stocks = loop.run_in_executor(executor,get_stocks)
        newsdf, stockdf= await asyncio.gather(news,stocks)
        sa = loop.run_in_executor(executor,sentiment_analysis, newsdf)
        sa_newsdf = await sa
        return sa_newsdf, stockdf
    
async def loop():
    thread = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
            newsdf, stockdf = await get_data()
            merged = await merge_news_stocks(newsdf,stockdf)
            model_async = thread.run_in_executor(executor,run_model,merged)
            result = await model_async
            print(merged)
            return {
                "merged": merged.to_dict(orient = "records"),
                "result": result
            }


if __name__ == "__main__":
    asyncio.run(loop(60))



