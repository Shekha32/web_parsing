
#parsing urls from a file in the data folder

import time
import urllib
import asyncio
import aiohttp
import aiofiles
#import requests
from bs4 import BeautifulSoup
from aiohttp import ClientSession
from fake_useragent import UserAgent


class ParseSite:

        __count = 0                                             #number of index file
        __result_file = './parse_result/'                       #path to result folder
        __urls = { 'success': set(), 'failure': [] }            #saved info about parsing urls
        __headers = {
                "accept": "*/*",
                "user-agent": UserAgent().chrome                #generate random/fake user-agent
                #"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
        }


        #get HTML-code from url
        async def __fetch_html ( self, url, session, retry=5 ) -> str:

                #print ( url )
                resp = await session.request ( method="GET", url=url, headers=self.__headers )

                if retry and resp.status in [ 429, 500, 502, 503, 504 ]:
                        time.sleep ( 2 )
                        await self.__fetch_html ( url=url, session=session, retry=retry-1 )

                resp.raise_for_status()                         #raise if status >= 400
                html = await resp.text()

                return html


        #parse HTML-code and creation of element
        async def __parse ( self, url, session ) -> dict:

                elem = {}
                
                try:
                        html = await self.__fetch_html ( url=url, session=session )
                
                except (
                        aiohttp.ClientError,
                        aiohttp.http_exceptions.HttpProcessingError,
                ) as error:

                        self.__urls [ 'failure' ].append ( { url: error } )

                else:
                        elem [ 'url' ] = url
                        elem [ 'html' ] = html
                        soup = BeautifulSoup ( html, "lxml" )

                        try:
                                elem [ 'name' ] = soup.find ( class_="page-title__title" ).text
                        
                        except ( urllib.error.URLError, ValueError, AttributeError ) as e:

                                elem.clear()
                                e = 'incorrect search settings' if type ( e ) == AttributeError else e
                                self.__urls [ 'failure' ].append ( { url: e } )

                return elem


        #commit element to index file if it's not empty
        async def __write_one ( self, url, session ) -> None:

                elem = await self.__parse ( url=url, session=session )

                if not elem:
                        return None

                self.__count += 1
                self.__urls [ 'success' ].add ( url )

                async with aiofiles.open ( f"{self.__result_file}{self.__count}_index.html", "w" ) as file:
                        await file.write ( f"{elem [ 'url' ]}\n{elem [ 'name' ]}\n{elem [ 'html' ]}\n" )


        #session opening and tasks list creation
        async def create_task ( self, urls ) -> None:

                async with ClientSession() as session:
                        tasks = []
                        [ tasks.append ( self.__write_one ( url=url, session=session ) ) for url in urls ]
                        await asyncio.gather ( *tasks )


#parsing urls from a file in the data folder
def parse_urls() -> None:

        ob = ParseSite()
        
        with open ( './data/habr.csv', 'r' ) as file:           #here you could change file to testfile named 'habr2.csv'
                next ( file )
                urls = set ( map ( str.strip, file ) )          #creation of urls tuple

        start_time = time.time()
        asyncio.run ( ob.create_task ( urls ) )
        finish_time = time.time() - start_time
        print ( 'time: ', finish_time, 'sec =', finish_time / 60.0, 'min' )
