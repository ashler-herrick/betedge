"""
ThetaData Terminal Authentication Module

This module provides functionality to authenticate and manage the ThetaData Terminal
using subprocess calls to handle the Java .jar execution.
"""

import subprocess
import time
from typing import Optional
import logging

from betedge_data.theta.config import ThetaTerminalConfig

logger = logging.getLogger(__name__)


class ThetaTerminalManager:
    """Manages ThetaData Terminal authentication and process lifecycle."""

    def __init__(self):
        """
        Initialize ThetaTerminal manager.

        Args:
            config: ThetaTerminalConfig instance, uses default if None
        """
        self.config = ThetaTerminalConfig()
        self.jar_path = self.config.jar_path
        self.username = self.config.username
        self.password = self.config.password
        self.process: Optional[subprocess.Popen] = None
        self.is_authenticated = False

    def authenticate(self) -> bool:
        """
        Authenticate with ThetaData Terminal.

        Returns:
            bool: True if authentication successful, False otherwise
        """
        if not self.jar_path.exists():
            logger.error(f"ThetaTerminal.jar not found at {self.jar_path}")
            return False

        try:
            # Start the Java process
            self.process = subprocess.Popen(
                ["java", "-jar", str(self.jar_path)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            # Wait for prompts and send credentials interactively
            return self._interactive_authentication()

        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False

    def _interactive_authentication(self) -> bool:
        """
        Handle interactive authentication flow with ThetaTerminal.

        Returns:
            bool: True if authentication successful
        """
        try:
            # Wait for username prompt
            if not self._wait_for_prompt("username (email)"):
                return False

            # Send username
            username = self.username or ""
            self.process.stdin.write(f"{username}\n")
            self.process.stdin.flush()
            logger.info(f"Sent username: {'[provided]' if username else '[blank for free access]'}")

            # Wait for password prompt
            if not self._wait_for_prompt("password"):
                return False

            # Send password
            password = self.password or ""
            self.process.stdin.write(f"{password}\n")
            self.process.stdin.flush()
            logger.info(f"Sent password: {'[provided]' if password else '[blank for free access]'}")

            # Now wait for connection confirmation
            return self._wait_for_connection()

        except Exception as e:
            logger.error(f"Interactive authentication failed: {e}")
            return False

    def _wait_for_prompt(self, prompt_text: str, timeout: int = 10) -> bool:
        """
        Wait for a specific prompt from ThetaTerminal.

        Args:
            prompt_text: Text to look for in the prompt
            timeout: Maximum time to wait for prompt

        Returns:
            bool: True if prompt received
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.process.poll() is not None:
                logger.error("ThetaTerminal process terminated while waiting for prompt")
                return False

            try:
                output_line = self.process.stdout.readline()
                if output_line:
                    logger.info(f"ThetaTerminal: {output_line.strip()}")

                    # Check if this line contains the expected prompt
                    if prompt_text.lower() in output_line.lower():
                        return True

            except Exception as e:
                logger.warning(f"Error reading ThetaTerminal output: {e}")

            time.sleep(0.1)

        logger.error(f"Timeout waiting for prompt: {prompt_text}")
        return False

    def _wait_for_connection(self, timeout: Optional[int] = None) -> bool:
        """
        Wait for connection confirmation from ThetaTerminal.

        Args:
            timeout: Maximum time to wait for connection, uses config default if None

        Returns:
            bool: True if connected successfully
        """
        timeout = timeout or self.config.connection_timeout
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.process.poll() is not None:
                # Process terminated
                stderr_output = self.process.stderr.read()
                logger.error(f"ThetaTerminal process terminated: {stderr_output}")
                return False

            # Read available output
            try:
                # Use select or similar for non-blocking read in production
                output_line = self.process.stdout.readline()
                if output_line:
                    logger.info(f"ThetaTerminal: {output_line.strip()}")

                    # Check for connection indicators
                    if "CONNECTED" in output_line and ("FPSS" in output_line or "MDDS" in output_line):
                        self.is_authenticated = True
                        logger.info("ThetaTerminal authentication successful")
                        return True

                    # Check for authentication errors
                    if "FAILED" in output_line or "ERROR" in output_line:
                        logger.error(f"Authentication failed: {output_line.strip()}")
                        return False

            except Exception as e:
                logger.warning(f"Error reading ThetaTerminal output: {e}")

            time.sleep(0.1)

        logger.error("Authentication timeout")
        return False

    def is_running(self) -> bool:
        """Check if ThetaTerminal process is running and authenticated."""
        return self.process is not None and self.process.poll() is None and self.is_authenticated

    def shutdown(self) -> None:
        """Gracefully shutdown ThetaTerminal process."""
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=10)
                logger.info("ThetaTerminal process terminated successfully")
            except subprocess.TimeoutExpired:
                logger.warning("ThetaTerminal process did not terminate gracefully, killing...")
                self.process.kill()
            except Exception as e:
                logger.error(f"Error shutting down ThetaTerminal: {e}")
            finally:
                self.process = None
                self.is_authenticated = False
