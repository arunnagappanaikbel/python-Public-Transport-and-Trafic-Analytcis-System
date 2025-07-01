from src.config_loader import load_config
from src.data_ingestion import load_csv,load_json
from src.transformer import transform_trafic_data, enrich_transport_data, merge_data
from src.analyser import analyze_delay_vs_congestion
from src.logger import setup_logger
from src.file_handler import save_dataframe
import os
import datetime


CONFIG_Path = 'config/config.yaml'

def main():
    config = load_config(CONFIG_Path)
    logger = setup_logger(config['logging']['file'])
    
    try:
        logger.info(f'Loading traffic data and transport data started ') 
        traffic_json = load_json(os.path.join(config['paths']['traffic_data'], 'traffic_data.json'))
        transport_csv = load_csv(os.path.join(config['paths']['transport_data'], 'transport_data.csv'))
        logger.info(f'Loading traffic data and transport data completed ') 
        
        #Transformation
        logger.info(f'Transforming nested traffic data to dataframe started...') 
        traffic_data = transform_trafic_data(traffic_json)
        logger.info(f'Transforming nested traffic data to dataframe completed... total rows are: {traffic_data.count()}') 
        enriched_transport = enrich_transport_data(transport_csv)
        merged_df = merge_data(traffic_data, enriched_transport)  
        logger.info(f'merge data is completed ')      
        #Analyse
        analyse_df = analyze_delay_vs_congestion(merged_df)        
        #Load
        logger.info(f'Saving the dataframe started..')
        save_dataframe(merged_df, config['paths']['processed_dir'],'merged_data.csv')
        save_dataframe(analyse_df, config['paths']['output_dir'],'analysed_data.csv')
        logger.info(f'Saving the dataframe compeletd.. at ')
        
        logger.info(f'ETL process completed successfully')       
        
    except Exception as e:
        logger.exception(f'ETL pipeline failed: {e}')
        

if __name__ == '__main__':
    main()
    
#Analyze real-time performance and delays in public transportation systems using complex, 
# multi-nested JSON from live APIs and CSV files. Perform deep analytics on passenger traffic,
# route efficiency, congestion levels, average wait time, bus/train delays, and optimization metrics.

