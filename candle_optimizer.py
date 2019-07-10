import pandas as pd
import numpy as np
from nsepy import get_history
import datetime
import os
import glob
def consolidate_files(path,filename):
    prices_consol=pd.DataFrame()   
    
    for g in glob.glob(path+filename):
        #print(g)
        prices=pd.DataFrame()
        prices=pd.read_csv(g,names=['Symbol','Date','timestamp','Open','High','Low','Close','OI','Volume'] ,na_values=['null'], na_filter=True)
        prices.index=prices['Date'].astype(str)+' '+prices['timestamp'].astype(str)
        prices_consol=prices_consol.append(prices)
        prices=None
    
    return prices_consol

def Data_Fetch_agri(g,filen):
    df=consolidate_files(g,filen)
    
    df['Date2']=  df['Date'].astype(str)+' '+df['timestamp'].astype(str)
    
    df['Date2']=pd.to_datetime(df['Date2'],format='%Y%m%d %H:%M')
    
    df.sort_values(by='Date2',ascending=True,inplace=True)
    df['date']=df['Date2'].copy()

    df.index=df['Date2'].copy()
    
    return df


def resample_df2(df,targ_time_interval):

    df2=df.resample(targ_time_interval).agg({'Open': 'first', 
                                 'High': 'max', 
                                 'Low': 'min', 
                                 'Close': 'last',
                                 'Volume': 'sum'})
    df2.dropna(inplace=True,axis=0)
    return df2

def candle_score(lst_0,lst_1,lst_2):    
    
    O_0,H_0,L_0,C_0=lst_0[0],lst_0[1],lst_0[2],lst_0[3]
    O_1,H_1,L_1,C_1=lst_1[0],lst_1[1],lst_1[2],lst_1[3]
    O_2,H_2,L_2,C_2=lst_2[0],lst_2[1],lst_2[2],lst_2[3]
    
    DojiSize = 0.1
    
    doji=(abs(O_0 - C_0) <= (H_0 - L_0) * DojiSize)
    
    hammer=(((H_0 - L_0)>3*(O_0 -C_0)) &  ((C_0 - L_0)/(.001 + H_0 - L_0) > 0.6) & ((O_0 - L_0)/(.001 + H_0 - L_0) > 0.6))
    
    inverted_hammer=(((H_0 - L_0)>3*(O_0 -C_0)) &  ((H_0 - C_0)/(.001 + H_0 - L_0) > 0.6) & ((H_0 - O_0)/(.001 + H_0 - L_0) > 0.6))
    
    bullish_reversal= (O_2 > C_2)&(O_1 > C_1)&doji
    
    bearish_reversal= (O_2 < C_2)&(O_1 < C_1)&doji
    
    evening_star=(C_2 > O_2) & (min(O_1, C_1) > C_2) & (O_0 < min(O_1, C_1)) & (C_0 < O_0 )
    
    morning_star=(C_2 < O_2) & (min(O_1, C_1) < C_2) & (O_0 > min(O_1, C_1)) & (C_0 > O_0 )
    
    shooting_Star_bearish=(O_1 < C_1) & (O_0 > C_1) & ((H_0 - max(O_0, C_0)) >= abs(O_0 - C_0) * 3) & ((min(C_0, O_0) - L_0 )<= abs(O_0 - C_0)) & inverted_hammer
    
    shooting_Star_bullish=(O_1 > C_1) & (O_0 < C_1) & ((H_0 - max(O_0, C_0)) >= abs(O_0 - C_0) * 3) & ((min(C_0, O_0) - L_0 )<= abs(O_0 - C_0)) & inverted_hammer
    
    bearish_harami=(C_1 > O_1) & (O_0 > C_0) & (O_0 <= C_1) & (O_1 <= C_0) & ((O_0 - C_0) < (C_1 - O_1 ))
    
    Bullish_Harami=(O_1 > C_1) & (C_0 > O_0) & (C_0 <= O_1) & (C_1 <= O_0) & ((C_0 - O_0) < (O_1 - C_1))
    
    Bearish_Engulfing=((C_1 > O_1) & (O_0 > C_0)) & ((O_0 >= C_1) & (O_1 >= C_0)) & ((O_0 - C_0) > (C_1 - O_1 ))
    
    Bullish_Engulfing=(O_1 > C_1) & (C_0 > O_0) & (C_0 >= O_1) & (C_1 >= O_0) & ((C_0 - O_0) > (O_1 - C_1 ))
    
    Piercing_Line_bullish=(C_1 < O_1) & (C_0 > O_0) & (O_0 < L_1) & (C_0 > C_1)& (C_0>((O_1 + C_1)/2)) & (C_0 < O_1)

    Hanging_Man_bullish=(C_1 < O_1) & (O_0 < L_1) & (C_0>((O_1 + C_1)/2)) & (C_0 < O_1) & hammer

    Hanging_Man_bearish=(C_1 > O_1) & (C_0>((O_1 + C_1)/2)) & (C_0 < O_1) & hammer

    strCandle=''
    candle_score=0
    
    if doji:
        strCandle='doji'
    if evening_star:
        strCandle=strCandle+'/ '+'evening_star'
        candle_score=candle_score-1
    if morning_star:
        strCandle=strCandle+'/ '+'morning_star'
        candle_score=candle_score+1
    if shooting_Star_bearish:
        strCandle=strCandle+'/ '+'shooting_Star_bearish'
        candle_score=candle_score-1
    if shooting_Star_bullish:
        strCandle=strCandle+'/ '+'shooting_Star_bullish'
        candle_score=candle_score-1
    if    hammer:
        strCandle=strCandle+'/ '+'hammer'
    if    inverted_hammer:
        strCandle=strCandle+'/ '+'inverted_hammer'
    if    bearish_harami:
        strCandle=strCandle+'/ '+'bearish_harami'
        candle_score=candle_score-1
    if    Bullish_Harami:
        strCandle=strCandle+'/ '+'Bullish_Harami'
        candle_score=candle_score+1
    if    Bearish_Engulfing:
        strCandle=strCandle+'/ '+'Bearish_Engulfing'
        candle_score=candle_score-1
    if    bullish_reversal:
        strCandle=strCandle+'/ '+'Bullish_Engulfing'
        candle_score=candle_score+1
    if    bullish_reversal:
        strCandle=strCandle+'/ '+'bullish_reversal'
        candle_score=candle_score+1
    if    bearish_reversal:
        strCandle=strCandle+'/ '+'bearish_reversal'
        candle_score=candle_score-1
    if    Piercing_Line_bullish:
        strCandle=strCandle+'/ '+'Piercing_Line_bullish'
        candle_score=candle_score+1
    if    Hanging_Man_bearish:
        strCandle=strCandle+'/ '+'Hanging_Man_bearish'
        candle_score=candle_score-1
    if    Hanging_Man_bullish:
        strCandle=strCandle+'/ '+'Hanging_Man_bullish'
        candle_score=candle_score+1
        
    #return candle_score
    return candle_score,strCandle

def candle_df(df):
    #df_candle=first_letter_upper(df)
    df_candle=df.copy()
    df_candle['candle_score']=0
    df_candle['candle_pattern']=''


    for c in range(2,len(df_candle)):
        cscore,cpattern=0,''
        lst_2=[df_candle['Open'].iloc[c-2],df_candle['High'].iloc[c-2],df_candle['Low'].iloc[c-2],df_candle['Close'].iloc[c-2]]
        lst_1=[df_candle['Open'].iloc[c-1],df_candle['High'].iloc[c-1],df_candle['Low'].iloc[c-1],df_candle['Close'].iloc[c-1]]
        lst_0=[df_candle['Open'].iloc[c],df_candle['High'].iloc[c],df_candle['Low'].iloc[c],df_candle['Close'].iloc[c]]
        cscore,cpattern=candle_score(lst_0,lst_1,lst_2)    
        df_candle['candle_score'].iat[c]=cscore
        df_candle['candle_pattern'].iat[c]=cpattern
    
    df_candle['candle_cumsum']=df_candle['candle_score'].rolling(3).sum()
    
    return df_candle

def Combinations(lst_A,lst_B,lst_C):
    lst_combination=[]
    for a in lst_A:
        for b in lst_B:
            for c in lst_C:
                    lst_combination.append([a,b,c])
    return lst_combination


lst_symbol=['NC-RMSEED_F1.txt',
'NC-GUARGUM5_F1.txt',
'NC-KAPAS_F1.txt',
'NC-WHEATFAQ_F1.txt',
'NC-SYBEANIDR_F1.txt',
'NC-WHEAT_F1.txt',
'NC-TMCFGRNZM_F1.txt',
'NC-SUGARM_F1.txt',
'NC-MAIZERABI_F1.txt',
'NC-COCUDAKL_F1.txt',
'NC-MAIZEKHRIF_F1.txt',
'NC-GUARSEED10_F1.txt',
'NC-BARLEYJPR_F1.txt',
'NC-CASTOR_F1.txt',
'NC-SYOREF_F1.txt',
'NC-JEERAUNJHA_F1.txt',
'NC-CHANA_F1.txt',
'NC-DHANIYA_F1.txt']

lst_symbol=['ADANIPORTS_F1.txt',
'ASIANPAINT_F1.txt',
'AXISBANK_F1.txt',
'BAJAJ-AUTO_F1.txt',
'BAJFINANCE_F1.txt',
'BAJAJFINSV_F1.txt',
'BPCL_F1.txt',
'BHARTIARTL_F1.txt',
'INFRATEL_F1.txt',
'CIPLA_F1.txt',
'COALINDIA_F1.txt',
'DRREDDY_F1.txt',
'EICHERMOT_F1.txt',
'GAIL_F1.txt',
'GRASIM_F1.txt',
'HCLTECH_F1.txt',
'HDFCBANK_F1.txt',
'HEROMOTOCO_F1.txt',
'HINDALCO_F1.txt',
'HINDPETRO_F1.txt',
'HINDUNILVR_F1.txt',
'HDFC_F1.txt',
'ITC_F1.txt',
'ICICIBANK_F1.txt',
'IBULHSGFIN_F1.txt',
'IOC_F1.txt',
'INDUSINDBK_F1.txt',
'INFY_F1.txt',
'JSWSTEEL_F1.txt',
'KOTAKBANK_F1.txt',
'LT_F1.txt',
'M&M_F1.txt',
'MARUTI_F1.txt',
'NTPC_F1.txt',
'ONGC_F1.txt',
'POWERGRID_F1.txt',
'RELIANCE_F1.txt',
'SBIN_F1.txt',
'SUNPHARMA_F1.txt',
'TCS_F1.txt',
'TATAMOTORS_F1.txt',
'TATASTEEL_F1.txt',
'TECHM_F1.txt',
'TITAN_F1.txt',
'UPL_F1.txt',
'ULTRACEMCO_F1.txt',
'VEDL_F1.txt',
'WIPRO_F1.txt',
'YESBANK_F1.txt',
'ZEEL_F1.txt']

lst_freq=['10min','15min','30min']

lst_pattern=['Bearish_Engulfing',
'bearish_harami',
'bearish_reversal',
'shooting_Star_bearish',
'evening_star',
'Hanging_Man_bearish']

lst_pattern=['Piercing_Line_bullish',
            'shooting_Star_bullish',
            'Bullish_Engulfing',
            'Bullish_Harami',
            'bullish_reversal',
            'inverted_hammer',
            'morning_star',
            'Hanging_Man_bullish',
            'hammer',
            'doji']



lst= Combinations(lst_symbol,lst_freq,lst_pattern)


path='/Users/*************/***********/IntradayData/IntradayData_2018/'

print('script,freq,pattern,trx,Ist cnd return,IInd cnd return,IIIrd cnd return')
for l in lst:
    try:
        symb1=l[0]
        freq=l[1]
        pattern=l[2]
    
        filen=os.path.basename(path+symb1)
        df=Data_Fetch_agri(path,filen)
        df=resample_df2(df,freq)
        
        df=candle_df(df)
        
        i=0
        df['candle_s']=0
        while i<len(df):
            #print(df.iloc[i]['candle_pattern'])    
            cpattern=str(df.iloc[i]['candle_pattern'])
            cpattern_find=str(cpattern).rfind(pattern)
            if cpattern_find>0:
                #print(cpattern,' ',cpattern_find)
                df['candle_s'].iat[i]=1
            i=i+1
                
        df['pct_change']=df['Close'].pct_change()*100
        df['next_day_pct_change']=df['pct_change'].shift(-1)
        df['2_day_pct_change']=df['pct_change'].shift(-2)
        df['3_day_pct_change']=df['pct_change'].shift(-3)
        
        #df[['Close','candle_s']].plot(y=['Close','candle_s'],secondary_y=['candle_s'],figsize=(12,8))
        
        df2=df[df['candle_s']==1].copy()
        
        #df2[['next_day_pct_change','2_day_pct_change','3_day_pct_change']].sum()
        print(symb1,',',freq,',' ,pattern,',', int(df2['candle_s'].count()) ,',', float(df2[['next_day_pct_change']].sum()),',',float(df2[['2_day_pct_change']].sum()) ,',',float(df2[['3_day_pct_change']].sum()))
    except:
        pass
