from bs4 import BeautifulSoup
import pandas as pd
import asyncio
import aiohttp

import time


baseurl="https://stores.myraymond.com/mystore/raymondstore.php?store_id="

async def cleanadd(list):
    #takes address list and return a list with format [address,state,city,pin]
    tmp=[]
    for  i in range(0,len(list)):
        if 'Address:' in list[i]:
            tmp.append(list[i])
            continue
        if 'State' in list[i]:
            tmp.append(list[i])
            continue
        if 'City' in list[i]:
            tmp.append(list[i])
            continue
        if 'Pin' in list[i]:
            tmp.append(list[i])
            continue
    return tmp


async def scrape(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            
            #some pages have text which cnould not be decoded properly
            try:
                page= await resp.text()
            except:
                print("failed for ",url)
                return

            soup=BeautifulSoup(page,'lxml')


            try:
                storename=soup.find('h4').text.replace(':-','').strip()
            except:
                print("storename not found for ",url)
                return

            for l in soup.find_all('div',{'class':'add'}):
                tmp=l.text.split('\n')
                tmp=await cleanadd(tmp)


                address=tmp[0].split('\xa0')[1].strip()
                state=tmp[1].split('\xa0')[1].strip()
                city=tmp[2].split('\xa0')[1].strip()
                pin=tmp[3].split('\xa0')[1].strip()


            #some stores do not have information about contact
            try:
                phone=soup.find('div',{'class':'phone'})
                phone=','.join(phone.text.strip().split('\n')).strip()
            except:
                phone='-'


            #some stores do not have info about their timings
            try:
                timings=soup.find('div',{'class':'time'})
                timings=timings.text.strip()
            except:
                timings='-'
            # print(timings)

            #some stores do not have info about coordinates
            for l in soup.find('div',{'class':'get_direction'}):
                coordinates=l.get('href').split('query=')[1].strip()

                if coordinates==',':
                    coordinates='-'

                # print(coordinates)



            data=pd.DataFrame([{'Store Name':storename,'Address':address,'State':state,'City':city,'Pin Code':pin,'Timings':timings,'Coordinates(Latitude,Longitude)':coordinates,'Phone Number':phone}])
            data.to_csv("file1.csv",mode='a',header=False,index=False)
               




async def main():
    start=time.time()
    
    df=pd.DataFrame(columns=['Store Name','Address','State','City','Pin Code','Timings','Coordinates(Latitude,Longitude)','Phone Number'])
    df.to_csv("file1.csv",index=False)

    tasks=[]
    for i in range(1,1700):
        task = asyncio.create_task(scrape(baseurl+str(i)))
        tasks.append(task)

    await asyncio.gather(*tasks)

    end=time.time()
    
    print(end-start)



asyncio.run(main())








        
        
        