from etl.online.extractor import Extractor

if __name__ == '__main__':
    ex = Extractor(
        hospital =       'HCGH',
        lookback_hours = 25,
        jhapi_server =   'prod',
        jhapi_id =       '09487db62cdc41d0a6fafa57a2cd30f5',
        jhapi_secret =   '7e415e173a7149029606B508289D4799'
    )
    bp = ex.extract_bedded_patients()
    print(bp)
