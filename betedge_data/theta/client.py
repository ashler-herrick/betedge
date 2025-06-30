import logging
import time
from betedge_data.theta.auth import ThetaTerminalManager

logger = logging.getLogger()


def main():
    """Example usage of ThetaTerminalManager."""
    # Initialize with config from environment variables
    terminal = ThetaTerminalManager()

    try:
        logger.info("Starting ThetaTerminal authentication...")
        if terminal.authenticate():
            logger.info("Authentication successful! Terminal is ready for data requests.")

            # Keep the terminal running for data requests
            while terminal.is_running():
                time.sleep(1)

        else:
            logger.error("Authentication failed!")

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        terminal.shutdown()


if __name__ == "__main__":
    main()
