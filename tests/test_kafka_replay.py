#!/usr/bin/env python3
"""
Kafka Consumer Test Script for Time Series Replay

This script consumes messages from a Kafka topic containing historical option data
and verifies that the messages are in chronological order for proper time series replay.

Usage:
    python test_kafka_replay.py [--topic TOPIC] [--max-messages N] [--verbose]
"""

import argparse
import io
import logging
import signal
import sys
from typing import Dict, Optional, Tuple

import pyarrow.parquet as pq
import polars as pl
from confluent_kafka import Consumer, KafkaError, KafkaException
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rich console for pretty output
console = Console()


class KafkaReplayConsumer:
    """Kafka consumer for replaying and validating time series data."""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092", group_id: str = "replay-test-group"):
        """
        Initialize the Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            group_id: Consumer group ID
        """
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'enable.auto.commit': False,  # Manual commit for replay control
            'auto.offset.reset': 'earliest',  # Start from beginning
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 45000,  # 45 seconds
        }
        
        self.consumer = Consumer(self.config)
        self.running = True
        self.stats = {
            'messages_consumed': 0,
            'total_bytes': 0,
            'dates_seen': [],
            'chronological_errors': 0,
            'parse_errors': 0
        }
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        console.print("\n[yellow]Received shutdown signal, closing consumer...[/yellow]")
        self.running = False
    
    def subscribe(self, topic: str):
        """Subscribe to a Kafka topic."""
        self.consumer.subscribe([topic])
        console.print(f"[green]Subscribed to topic: {topic}[/green]")
    
    def parse_parquet_data(self, data: bytes) -> Optional[Tuple[int, Dict]]:
        """
        Parse Parquet data from message.
        
        Args:
            data: Parquet data as bytes
            
        Returns:
            Tuple of (row_count, sample_data) or None on error
        """
        try:
            # Create BytesIO buffer from data
            buffer = io.BytesIO(data)
            
            # Read Parquet file
            table = pq.read_table(buffer)
            df = pl.from_arrow(table)
            
            # Get basic statistics
            row_count = len(df)
            columns = list(df.columns)
            
            # Get sample data (first row if available)
            sample_data = {}
            if row_count > 0:
                sample_data = df.iloc[0].to_dict()
            
            return row_count, {
                'columns': columns,
                'sample': sample_data
            }
            
        except Exception as e:
            logger.error(f"Failed to parse Parquet data: {e}")
            return None
    
    def validate_chronological_order(self, current_date: str) -> bool:
        """
        Validate that dates are in chronological order.
        
        Args:
            current_date: Current message date (YYYYMMDD)
            
        Returns:
            True if order is maintained, False otherwise
        """
        if not self.stats['dates_seen']:
            return True
        
        last_date = self.stats['dates_seen'][-1]
        return current_date >= last_date
    
    def consume_messages(self, topic: str, max_messages: Optional[int] = None, verbose: bool = False):
        """
        Consume messages from the topic and validate ordering.
        
        Args:
            topic: Kafka topic to consume from
            max_messages: Maximum number of messages to consume
            verbose: Whether to show detailed output
        """
        self.subscribe(topic)
        
        console.print("\n[bold blue]🔄 Kafka Topic Replay Test[/bold blue]")
        console.print("=" * 50)
        console.print(f"Topic: {topic}")
        console.print(f"Consumer Group: {self.config['group.id']}")
        console.print(f"Bootstrap Servers: {self.config['bootstrap.servers']}\n")
        
        messages_processed = 0
        last_symbol = None
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Consuming messages...", total=None)
            
            while self.running and (max_messages is None or messages_processed < max_messages):
                try:
                    msg = self.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            console.print("\n[yellow]Reached end of partition[/yellow]")
                            break
                        else:
                            raise KafkaException(msg.error())
                    
                    # Process message
                    messages_processed += 1
                    self.stats['messages_consumed'] += 1
                    
                    # Extract headers
                    headers = {}
                    if msg.headers():
                        for key, value in msg.headers():
                            headers[key] = value.decode('utf-8') if value else None
                    
                    # Extract key
                    key = msg.key().decode('utf-8') if msg.key() else None
                    
                    # Get message data
                    data = msg.value()
                    data_size = len(data) if data else 0
                    self.stats['total_bytes'] += data_size
                    
                    # Extract date and symbol from headers or key
                    date = headers.get('date', '')
                    symbol = headers.get('root', '')
                    if not symbol and key:
                        # Try to extract from key format: SYMBOL_DATE
                        parts = key.split('_')
                        if len(parts) >= 2:
                            symbol = parts[0]
                            date = parts[1]
                    
                    if symbol:
                        last_symbol = symbol
                    
                    # Validate chronological order
                    order_ok = self.validate_chronological_order(date)
                    if not order_ok:
                        self.stats['chronological_errors'] += 1
                    
                    if date:
                        self.stats['dates_seen'].append(date)
                    
                    # Parse Parquet data
                    parquet_info = None
                    if data:
                        parquet_info = self.parse_parquet_data(data)
                        if not parquet_info:
                            self.stats['parse_errors'] += 1
                    
                    # Update progress
                    progress.update(task, description=f"Processing message {messages_processed} (Date: {date})")
                    
                    # Display message details
                    if verbose or not order_ok:
                        console.print(f"\n[bold]Message {messages_processed}:[/bold]")
                        console.print(f"  Date: {date}")
                        console.print(f"  Symbol: {symbol}")
                        console.print(f"  Key: {key}")
                        console.print(f"  Size: {data_size:,} bytes")
                        
                        if parquet_info:
                            row_count, info = parquet_info
                            console.print(f"  Rows: {row_count:,}")
                            console.print(f"  Columns: {', '.join(info['columns'][:5])}...")
                        
                        if order_ok:
                            console.print("  [green]✓ Chronological order maintained[/green]")
                        else:
                            console.print("  [red]✗ Chronological order violation![/red]")
                    
                    # Commit offset
                    self.consumer.commit(asynchronous=False)
                    
                except Exception as e:
                    logger.error(f"Error consuming message: {e}")
                    if not self.running:
                        break
        
        # Display summary
        self._display_summary(last_symbol)
    
    def _display_summary(self, symbol: Optional[str] = None):
        """Display consumption summary."""
        console.print("\n[bold]Summary:[/bold]")
        console.print("=" * 50)
        
        # Create summary table
        table = Table(show_header=False, box=None)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="white")
        
        table.add_row("Total messages", f"{self.stats['messages_consumed']:,}")
        table.add_row("Total data size", f"{self.stats['total_bytes']:,} bytes")
        
        if self.stats['dates_seen']:
            date_range = f"{self.stats['dates_seen'][0]} to {self.stats['dates_seen'][-1]}"
            table.add_row("Date range", date_range)
            table.add_row("Unique dates", str(len(set(self.stats['dates_seen']))))
        
        if symbol:
            table.add_row("Symbol", symbol)
        
        table.add_row("Parse errors", str(self.stats['parse_errors']))
        table.add_row("Chronological errors", str(self.stats['chronological_errors']))
        
        console.print(table)
        
        # Overall result
        if self.stats['chronological_errors'] == 0 and self.stats['parse_errors'] == 0:
            console.print("\n[bold green]✅ All messages in correct chronological order![/bold green]")
            console.print("[green]✅ All Parquet data successfully parsed![/green]")
        else:
            if self.stats['chronological_errors'] > 0:
                console.print(f"\n[bold red]❌ Found {self.stats['chronological_errors']} chronological order violations![/bold red]")
            if self.stats['parse_errors'] > 0:
                console.print(f"[bold red]❌ Failed to parse {self.stats['parse_errors']} messages![/bold red]")
    
    def close(self):
        """Close the consumer."""
        self.consumer.close()
        console.print("\n[green]Consumer closed successfully[/green]")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Kafka Consumer Test Script for Time Series Replay")
    parser.add_argument(
        "--topic",
        default="historical-option-aapl-quote-15m",
        help="Kafka topic to consume from (default: historical-option-aapl-quote-15m)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)"
    )
    parser.add_argument(
        "--group-id",
        default="replay-test-group",
        help="Consumer group ID (default: replay-test-group)"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        help="Maximum number of messages to consume"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed output for each message"
    )
    
    args = parser.parse_args()
    
    # Create consumer
    consumer = KafkaReplayConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id
    )
    
    try:
        # Start consuming
        consumer.consume_messages(
            topic=args.topic,
            max_messages=args.max_messages,
            verbose=args.verbose
        )
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        sys.exit(1)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()