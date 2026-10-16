"""Time zone information.

Maps the time-zone abbreviations that services put beside a local date and
time to UTC offsets. Several abbreviations name more than one zone worldwide,
so the table follows the code lists the services draw from rather than common
usage:

* the WQX TimeZone domain, used by the Water Quality Portal
  (``tests/data/wqx_timezones.csv``), and
* the USGS NWIS time-zone list, used by the USGS Samples database and by the
  USGS records the Water Quality Portal passes through
  (``tests/data/usgs_timezones.xml``).

Where the two lists disagree -- ``SST`` (American Samoa in WQX, Swedish Summer
Time in USGS) and ``BST`` (retired Bering Standard Time in WQX, British Summer
Time in USGS) -- WQX wins: the Water Quality Portal returns ``SST`` for American
Samoa, and USGS records write that zone as ``ZP-11``. Abbreviations in
neither list keep their common meaning. ``tests/timezones_test.py`` checks the
table against both lists.
"""

tz_str = """-1200 Y IDLW
-1100 X BST NUT SST ZP-11
-1000 W AHST CKT HAST HST TAHT TKT
-0900 V AKST GAMT GIT HADT HDT HNY YST
-0800 U AKDT CIST HAY HNP PST PT
-0700 T HAP HNR MST PDT
-0600 S CST GALT HAR HNC MDT
-0500 R CDT COT ECT EST ET HAC HNE PET
-0400 Q AST BOT CLT COST EDT FKT GYT HAE HNA PYT
-0300 P ADT ART BRT CLST FKST GFT HAA PMST PYST SRT UYT WGT ZP-3
-0200 O BRST FNT PMDT UYST WGST ZP-2
-0100 N AZOT CVT EGT WAT
+0000 Z EGST GMT UTC WET WT
+0100 A CET DFT DNT DST FST MET MEWT MEZ NOR SET SWT WEDT WEST WETDST
+0200 B CAT CEDT CEST CETDST EET FWT IST MEST METDST SAST
+0300 C BT EAT EEDT EEST EETDST IDT MSK
+0400 D AMT AZT GET KUYT MSD MUT RET SAMT SCT ZP4
+0500 E AMST AQTT AZST HMT MAWT MVT PKT TFT TJT TMT UZT YEKT ZP5
+0600 F ALMT BIOT BTT IOT KGT NOVT OMST YEKST ZP6
+0700 G CXT DAVT HOVT ICT KRAT NOVST OMSST THA WAST WIB
+0800 H ACT AWST BDT BNT CCT HKT IRKT KRAST MYT PHT SGT ULAT WADT WITA WST
+0900 I AWDT AWSST IRKST JST KST PWT TLT WDT WIT YAKT
+1000 K AEST ChST EAST GST LIGT PGT VLAT YAKST YAPT
+1100 L AEDT AESST EASST LHDT MAGT NCT PONT SBT VLAST VUT ZP11
+1200 M ANAST ANAT FJT GILT IDLE MAGST MHT NZST NZT PETST PETT TVT WFT
+1300 FJST NZDT
+1030 ACDT ACSST CADT LHST SADT
+0930 ACST CAST SAT
+0830 MT
+0730 JT
+0630 MMT
+0545 NPT
+0530 SLT
+0430 AFT IRDT
+0330 IRST IT
-0230 HAT NDT
-0330 HNT NFT NST NT
-0430 HLV VET
-0930 MART MIT"""

tz = {}
for tz_descr in map(str.split, tz_str.split("\n")):
    tz_offset = tz_descr[0]
    for tz_code in tz_descr[1:]:
        tz[tz_code] = tz_offset
