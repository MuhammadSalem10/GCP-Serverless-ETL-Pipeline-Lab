"""
Apache Beam pipeline for ETL processing of sales data.
This pipeline demonstrates modern data engineering practices with Apache Beam.
"""

import argparse
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToBigQuery


class SalesDataTransform(beam.DoFn):
    """Transform sales data with proper error handling."""
    
    def process(self, element):
        """Process each CSV row and calculate total_sale."""
        try:
            # Skip header row
            if element.startswith('id,'):
                return
            
            parts = element.split(',')
            if len(parts) != 5:
                logging.warning(f"Skipping malformed row: {element}")
                return
            
            id_val, product, price_str, quantity_str, sale_date_str = parts
            
            # Data type conversions with error handling
            try:
                price = float(price_str)
                quantity = int(quantity_str)
                total_sale = price * quantity
                
                # Convert date format for BigQuery
                sale_date = datetime.strptime(sale_date_str, '%Y-%m-%d').date()
                
            except (ValueError, TypeError) as e:
                logging.warning(f"Data conversion error for row {element}: {e}")
                return
            
            # Return transformed record
            yield {
                'id': id_val,
                'product': product,
                'price': price,
                'quantity': quantity,
                'sale_date': sale_date.isoformat(),
                'total_sale': total_sale
            }
            
        except Exception as e:
            logging.error(f"Unexpected error processing row {element}: {e}")


def run_pipeline(argv=None):
    """Main pipeline execution function."""
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='Input CSV file path')
    parser.add_argument('--output', required=True, help='BigQuery output table')
    #parser.add_argument('--project', required=True, help='GCP Project ID')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    
    # BigQuery table schema
    table_schema = {
        'fields': [
            {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'product', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'price', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'quantity', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'sale_date', 'type': 'DATE', 'mode': 'REQUIRED'},
            {'name': 'total_sale', 'type': 'FLOAT', 'mode': 'REQUIRED'}
        ]
    }
    
    # Build and run pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read CSV' >> ReadFromText(known_args.input)
            | 'Transform Data' >> beam.ParDo(SalesDataTransform())
            | 'Write to BigQuery' >> WriteToBigQuery(
                known_args.output,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()