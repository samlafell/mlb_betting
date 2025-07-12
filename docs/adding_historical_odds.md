Now, let's add in the odds data to this!

We are now needing to add in a different site.

Site: Sportsbook Review

Request Headers:
```
GET /betting-odds/mlb-baseball/totals/full-game/?date=2021-04-04 HTTP/2
Host: www.sportsbookreview.com
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:140.0) Gecko/20100101 Firefox/140.0
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8
Accept-Language: en-US,en;q=0.5
Accept-Encoding: gzip, deflate, br, zstd
Referer: https://www.reddit.com/
DNT: 1
Connection: keep-alive
Cookie: SSID_rs35=CQCwLB0AAAAAAABt_GpokWVBIm38amgBAAAAAAAAAAAAbfxqaABtBw; SSSC_rs35=985.G7524103673132180881.1|0.0; SSRT_rs35=b_xqaAADAA; CookieConsent={stamp:%27-1%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27implied%27%2Cver:1%2Cutc:1751841903047%2Cregion:%27US-37%27}; _v_id_l={%22_v_id%22:%2247871276582357131751841903244%22%2C%22_la%22:1751841934826}; kndctr_9CE579FD5DCD8B590A495E09_AdobeOrg_identity=CiYwMzc4MDcwOTk5MzMyMzEwNjc1MzgxMTMwNzg5NzY1MzA4MDI5NFISCOepqI_-MhABGAEqA09SMjAA8AHnqaiP_jI=; kndctr_9CE579FD5DCD8B590A495E09_AdobeOrg_cluster=or2; AMCV_9CE579FD5DCD8B590A495E09%40AdobeOrg=MCMID|03780709993323106753811307897653080294
Upgrade-Insecure-Requests: 1
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: cross-site
Sec-Fetch-User: ?1
Sec-GPC: 1
Priority: u=0, i
TE: trailers
```

URLs:

Total:
@https://www.sportsbookreview.com/betting-odds/mlb-baseball/totals/full-game/?date=2025-07-01 

Moneyline: 
@https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date=2025-07-01 

Spread:
@https://www.sportsbookreview.com/betting-odds/mlb-baseball/pointspread/full-game/?date=2025-07-01 

# Every page
## Xpath of the table we care about:
```
//*[@id="tbody-mlb"] 
```

## row  1:
```
/html/body/div/main/div[5]/section/div[3]/div[1]
```

## contains the game time (xpath):
```
/html/body/div/main/div[5]/section/div[3]/div[1]/div[1]/div[1]
```

## Contains the teams, starting pitchers (handiness) and final score:
```
/html/body/div/main/div[5]/section/div[3]/div[1]/div[1]/div[2]
```

- handiness is contained inside parenthesis ()
- teams are identified by their abbreviations which we should use existing services or tables to match abbreviations

Then odds are provided for 5 sportsbooks, we should check the sportsbooks to make sure they dont change, xpath entrypoint:
```
/html/body/div/main/div[5]/section/div[2]/div[2]/div[2]/div
```

Then in this path:
```
/html/body/div/main/div[5]/section/div[2]/div[2]/div[2]/div/div[1]/a
```

I need to pull out the part of the html tag that is called "data-aatracker=", from here the last word that is the value of the tag will tell us the sportsbook.
In the example I see, the 5 books as:
- betMGHM
- fanduel
- caesars
- bet365
- draftkings
- betrivers


Now here we want to pull the value and odds for each sports book.
In the example I see, some are null. So it's okay if values are null.

I see the outer html of the first row for the 2nd sports book (becasue the first was null, 2nd book is FanDuel):
```
<span role="button" class="me-2 fs-9 OddsCells_bestOdd__zUbph"><span class="me-1 me-lg-2">-1.5</span><span>+104</span></span>
```

and the 2nd row for the 2nd book (outer html):
```
<span role="button" class="me-2 "><span class="me-1 me-lg-2 fs-9">+1.5</span><span class="fs-9">-125</span></span>
```


# Specific to the spread page

## Spread content (xpath):
```
/html/body/div/main/div[5]/section/div[3]/div[1]/div[3]/div/div[1]/div[2]/div
```
- On other pages this will be ML or Total info, probably located at the same xpath location...
(moneyline page outer html:
```
<div class="d-flex flex-column align-items-end justify-content-center"><div class="d-flex align-items-center text-center GameRows_offline__f5jJ8"><span data-cy="odd-grid-opener-homepage" class="me-2"><span class="me-1 me-2 fs-9 undefined"></span><span class="fs-9 undefined">-164</span></span></div><div class="d-flex align-items-center text-center border-bottom-0"><span data-cy="odd-grid-opener-homepage" class="me-2"><span class="me-1 me-2 fs-9 undefined"></span><span class="fs-9 undefined">+138</span></span></div></div>
```
) which shows up -164 and +138 odds

Opening spread line for the team on the top row xpath:
```
/html/body/div/main/div[5]/section/div[3]/div[1]/div[3]/div/div[1]/div[2]/div/div[1]/span
```
(matches up to:
```
/html/body/div/main/div[5]/section/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/a
```
)

# Specific to the moneyline page

## Wager %

### outer html
```
<div class="d-flex col-6 justify-content-end align-items-center  border-end GameRows_consensusColumn__AOd1q GameRows_consensus__2AvPi GameRows_numbersContainer__Ld_4v" data-vertical-sbid="-2"><div class="d-flex flex-column align-items-center justify-content-end fs-9 "><div class="d-flex align-items-center w-100 fs-9 undefined justify-content-end "><span data-cy="odd-grid-opener-homepage" class="me-2"><span class="opener">66%</span></span></div><div class="d-flex align-items-center w-100 fs-9 undefined justify-content-end border-bottom-0"><span data-cy="odd-grid-opener-homepage" class="me-2"><span class="opener">34%</span></span></div></div></div>
```

- This is only on the Moneyline page and relates to public bet % (consensus across multiple books)


# Detailed Matchup information

## Link
Outer HTML
```html
<a rel="" data-cy="button-grid-linehistory" class="fs-9 py-2 ps-1 text-primary" href="/betting-odds/mlb-baseball/line-history/342528/"><i class="mat-icon-line-chart_stacked icon-blue mat-icon-size-12"></i> <span class="visually-hidden">Icon for rating guide</span></a>
```

- To the base URL (https://www.sportsbookreview.com) we will add on the href from this outer html.
- example:
    - https://www.sportsbookreview.com/betting-odds/mlb-baseball/line-history/342528


## Across all the detailed matchup page information
- To know how many rows we have for the market (total / moneyline / spread), we can count how many flex items are in the table:

```html
<div class="d-flex flex-column w-100 mb-5 mb-lg-0 mt-2i GameMatchup_list__9RD67 Rows_Rows__wF9TM Rows_SportbooksResult__clsEx"><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R"></div><div class="d-flex justify-content-center text-center h5 text-dark Rows_RowData__KkO2R">Opener</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R"></div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R"><b>Time</b></div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R"><b>NYY</b></div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R"><b>TOR</b></div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">06/30 3:13 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 -102</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -118</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R"><b>Time</b></div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R"><b>NYY</b></div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R"><b>TOR</b></div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 3:18 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 2:53 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +105</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -126</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 2:38 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 2:28 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +102</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -122</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 1:53 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 1:13 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +102</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -122</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 12:58 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 12:29 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +102</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -122</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 12:13 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 12:08 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +106</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -128</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 11:38 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 11:33 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +102</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -122</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 11:23 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 11:08 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +105</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -126</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 11:03 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +106</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -128</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 10:23 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +105</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -126</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 10:03 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +106</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -128</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 9:58 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 9:23 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +105</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -126</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 9:13 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 9:03 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +105</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -126</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 7:53 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +106</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -128</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 7:38 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +112</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -134</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 5:43 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +106</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -128</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 12:58 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 12:43 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +105</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -126</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 12:33 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 12:28 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +105</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -126</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">07/01 12:23 am</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +106</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -128</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">06/30 7:38 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +105</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -126</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">06/30 4:18 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 +104</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -125</div></div><div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">06/30 3:13 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 -102</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -118</div></div></div>
```

- This table had 36 flex items
- The 3rd flex item is the opening line
- The 36th flex item is also the opening line
- The 2nd flex item will tell us which teams are participating (in this example it's Yankees and Toronto)


## Point spread
first, we will imitate a click on the point spread button so we make sure we are grabbing point spread info
outer html:
```html
<li role="button" class="dropdown-item  fs-9 me-3 d-flex d-lg-inline-block justify-content-center text-center mt-0 pe-3  Dropdown_active__GNX8k" data-format="pointspread">Point Spread</li>
```

Now that we have clicked on this, we can get the opening Spread line.
```html
<div class="d-flex align-content-center align-items-center justify-content-center fs-9 py-2 w-100 Rows_Row__9F5tn"><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">06/30 3:13 pm</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">-1.5 -102</div><div class="d-flex justify-content-center text-center Rows_RowData__KkO2R">+1.5 -118</div></div>
```

xpath:
/html/body/div/main/div[4]/aside/div[2]/div/div[3]/div[2]



- the team that has the -1.5 -102 is associated wtih the first column
- the identifier for that team is at:
```html
<div class="d-flex justify-content-center text-center Rows_RowData__KkO2R"><b>NYY</b></div>
```
(the xpath for the team identifier:
```
/html/body/div/main/div[4]/aside/div[2]/div/div[2]/div[2]
```
)


## Moneyline page

Once we imitate a click on the moneyline part of the page (outer html):
```html
<li role="button" class="dropdown-item  fs-9 me-3 d-flex d-lg-inline-block justify-content-center text-center mt-0 pe-3  Dropdown_active__GNX8k" data-format="money-line">Money Line</li>
```

(this is what it looks like when moneyline page is not active:
```html
<li role="button" class="dropdown-item  fs-9 me-3 d-flex d-lg-inline-block justify-content-center text-center mt-0 pe-3  " data-format="money-line">Money Line</li>
```
)


## Total page
This is what it'll look like when the totals page is not activated
```html
<li role="button" class="dropdown-item  fs-9 me-3 d-flex d-lg-inline-block justify-content-center text-center mt-0 pe-3  " data-format="totals">Totals</li>
```

And when it is activated:
```html
<li role="button" class="dropdown-item  fs-9 me-3 d-flex d-lg-inline-block justify-content-center text-center mt-0 pe-3  Dropdown_active__GNX8k" data-format="totals">Totals</li>
```


## Summary of the detailed matchup pages

Msot of the information across the pages is the same, we just have to imitate a click of the button to then grab the right information.

General notes:
- Times are listed in EST

moneyline notes:
- Plus odds do explicitly have a + sign, and favorites have - signs in front