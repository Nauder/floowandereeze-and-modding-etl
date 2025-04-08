"""GUI version of the ETL process for extracting and processing card data."""

import logging
import tkinter as tk
from tkinter import ttk, scrolledtext
import threading
import queue

from services.data_service import DataService
from services.decode_service import DecodeService
from util import GAME_PATH, NUM_THREADS, clear_directory


class RedirectText:
    """A class to redirect text output to a queue for thread-safe GUI updates.

    This class is used to capture text output and send it to a queue, which is then
    processed by the GUI to update the output text area in a thread-safe manner.
    """

    def __init__(self, text_widget, queue):
        """Initialize the RedirectText instance.

        Args:
            text_widget: The text widget to update.
            queue: The queue to put text updates into.
        """
        self.text_widget = text_widget
        self.queue = queue

    def write(self, string):
        """Write text to the queue.

        Args:
            string: The text to write.
        """
        self.queue.put(string)

    def flush(self):
        """Required for compatibility with logging.StreamHandler."""
        pass


class ETLGUI:
    """Main GUI class for the ETL process.

    This class provides a graphical user interface for running the ETL process,
    allowing users to select which steps to run and view the output in real-time.
    """

    def __init__(self, root):
        """Initialize the ETL GUI.

        Args:
            root: The root Tkinter window.
        """
        self.root = root
        self.root.title("Floowandereeze ETL")
        self.root.geometry("800x600")

        # Configure dark theme colors
        self.bg_color = "#2b2b2b"
        self.fg_color = "#ffffff"
        self.accent_color = "#3c3f41"
        self.text_bg = "#3c3f41"
        self.text_fg = "#ffffff"
        self.separator_color = "#4a4a4a"

        # Configure root window
        self.root.configure(bg=self.bg_color)

        # Create main frame
        self.main_frame = ttk.Frame(root, padding="10")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Create style for dark theme
        self.style = ttk.Style()
        self.style.configure("TFrame", background=self.bg_color)
        self.style.configure(
            "TLabel", background=self.bg_color, foreground=self.fg_color
        )
        self.style.configure(
            "TCheckbutton", background=self.bg_color, foreground=self.fg_color
        )
        self.style.configure("TButton", background=self.accent_color)
        self.style.configure(
            "TLabelframe", background=self.bg_color, foreground=self.fg_color
        )
        self.style.configure(
            "TLabelframe.Label", background=self.bg_color, foreground=self.fg_color
        )

        # Create configuration info frame
        config_frame = ttk.Frame(self.main_frame)
        config_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

        # Add configuration labels
        game_path_label = ttk.Label(
            config_frame, text=f"Game Path: {GAME_PATH}", wraplength=700
        )
        game_path_label.grid(row=0, column=0, sticky=tk.W)

        threads_label = ttk.Label(config_frame, text=f"Threads: {NUM_THREADS}")
        threads_label.grid(row=1, column=0, sticky=tk.W, pady=(5, 0))

        # Create checkboxes for each step
        self.steps = [
            ("Get IDs", self.get_ids),
            ("Decode Card Data", self.decode_card_data),
            ("Get Card Names", self.get_card_data),
            ("Clean Data", self.clean_data),
            ("Write Data", self.write_data),
            ("Remove Temporary Files", self.remove_temp_files),
        ]

        self.checkboxes = []
        for i, (text, _) in enumerate(self.steps):
            var = tk.BooleanVar(value=True)
            cb = ttk.Checkbutton(self.main_frame, text=text, variable=var)
            cb.grid(row=i + 1, column=0, sticky=tk.W, pady=5)
            self.checkboxes.append((var, cb))

        # Add separator
        separator = ttk.Separator(self.main_frame, orient="horizontal")
        separator.grid(row=len(self.steps) + 1, column=0, sticky=(tk.W, tk.E), pady=10)

        # Add field sorting checkbox
        self.sort_fields_var = tk.BooleanVar(value=True)
        self.sort_fields_cb = ttk.Checkbutton(
            self.main_frame,
            text="Sort Fields (requires user input)",
            variable=self.sort_fields_var,
        )
        self.sort_fields_cb.grid(row=len(self.steps) + 2, column=0, sticky=tk.W, pady=5)

        # Create run button
        self.run_button = ttk.Button(
            self.main_frame, text="Run Selected Steps", command=self.run_selected_steps
        )
        self.run_button.grid(row=len(self.steps) + 3, column=0, pady=10)

        # Create output area
        self.output_frame = ttk.LabelFrame(self.main_frame, text="Output", padding="5")
        self.output_frame.grid(
            row=len(self.steps) + 4, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=10
        )

        self.output_text = scrolledtext.ScrolledText(
            self.output_frame,
            height=1,  # Start with minimal height, will expand
            width=80,
            bg=self.text_bg,
            fg=self.text_fg,
            insertbackground=self.text_fg,  # Cursor color
            state="disabled",  # Make it read-only
        )
        self.output_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure grid weights for resizing
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, weight=1)

        # Configure main frame to expand
        self.main_frame.grid_rowconfigure(0, weight=0)  # Config info
        self.main_frame.grid_rowconfigure(1, weight=0)  # Steps
        self.main_frame.grid_rowconfigure(2, weight=0)  # Steps
        self.main_frame.grid_rowconfigure(3, weight=0)  # Steps
        self.main_frame.grid_rowconfigure(4, weight=0)  # Steps
        self.main_frame.grid_rowconfigure(5, weight=0)  # Steps
        self.main_frame.grid_rowconfigure(6, weight=0)  # Steps
        self.main_frame.grid_rowconfigure(7, weight=0)  # Separator
        self.main_frame.grid_rowconfigure(8, weight=0)  # Sort fields
        self.main_frame.grid_rowconfigure(9, weight=0)  # Run button
        self.main_frame.grid_rowconfigure(10, weight=1)  # Output frame (expand)
        self.main_frame.grid_columnconfigure(0, weight=1)

        # Configure output frame to expand
        self.output_frame.grid_rowconfigure(0, weight=1)
        self.output_frame.grid_columnconfigure(0, weight=1)

        # Queue for thread-safe text updates
        self.queue = queue.Queue()

        # Initialize services
        self.data_service = DataService()
        self.decode_service = DecodeService()

        # Set up logging
        self.setup_logging()

        # Start periodic queue check
        self.check_queue()

        # Bind window resize event
        self.root.bind("<Configure>", self.on_window_resize)

    def on_window_resize(self, event):
        """Handle window resize events.

        Updates the output text width based on the new window size.

        Args:
            event: The resize event containing the new window dimensions.
        """
        if event.widget == self.root:
            # Calculate new width based on window size
            new_width = (event.width - 40) // 8  # Approximate character width
            self.output_text.configure(width=max(40, new_width))

    def setup_logging(self):
        """Set up logging to redirect output to the GUI text area."""
        # Create a logger
        self.logger = logging.getLogger("gui_main")
        self.logger.setLevel(logging.INFO)

        # Create a handler that writes to our redirected text
        handler = logging.StreamHandler(RedirectText(self.output_text, self.queue))
        handler.setFormatter(
            logging.Formatter("[%(asctime)s|%(name)s|%(levelname)s]: %(message)s")
        )
        self.logger.addHandler(handler)

    def check_queue(self):
        """Check the queue for new text updates and display them in the output area."""
        try:
            while True:
                msg = self.queue.get_nowait()
                self.output_text.configure(state="normal")  # Enable writing
                self.output_text.insert(tk.END, msg)
                self.output_text.see(tk.END)
                self.output_text.configure(state="disabled")  # Disable writing
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.check_queue)

    def get_ids(self):
        """Run the get_ids step of the ETL process."""
        self.logger.info("Getting ids...")
        self.data_service.get_ids()
        self.logger.info("Done")

    def decode_card_data(self):
        """Run the decode_card_data step of the ETL process."""
        self.logger.info("Decoding card data...")
        self.decode_service.decrypt_desc_indx_name()
        self.decode_service.decrypt_ids()
        self.logger.info("Done")

    def get_card_data(self):
        """Run the get_card_data step of the ETL process."""
        self.logger.info("Getting card names...")
        self.data_service.get_card_data()
        self.logger.info("Done")

    def clean_data(self):
        """Run the clean_data step of the ETL process.

        Uses the sort_fields_var to determine whether to sort fields.
        """
        self.logger.info("Cleaning data...")
        self.data_service.clean_data(sort_fields=self.sort_fields_var.get())
        self.logger.info("Done")

    def write_data(self):
        """Run the write_data step of the ETL process."""
        self.logger.info("Writing data...")
        self.data_service.write_data()
        self.logger.info("Done")

    def remove_temp_files(self):
        """Run the remove_temp_files step of the ETL process."""
        self.logger.info("Removing temporary files...")
        clear_directory("./etl/services/temp")
        self.logger.info("Done")

    def run_selected_steps(self):
        """Run the selected ETL steps in order.

        This method runs in a separate thread to keep the GUI responsive.
        It disables the run button while processing and re-enables it when done.
        """
        self.run_button.config(state=tk.DISABLED)
        self.output_text.delete(1.0, tk.END)

        def run_steps():
            try:
                self.logger.info("Starting ETL process...")
                self.logger.info('Game path: "%s"', GAME_PATH)
                self.logger.info("Threads to use: %d", NUM_THREADS)

                for (var, _), (_, func) in zip(self.checkboxes, self.steps):
                    if var.get():
                        func()

                self.logger.info("ETL Finished")
            except Exception as e:
                self.logger.error(f"Error during ETL process: {str(e)}")
            finally:
                self.root.after(0, lambda: self.run_button.config(state=tk.NORMAL))

        # Run in a separate thread to keep GUI responsive
        threading.Thread(target=run_steps, daemon=True).start()


def main():
    """Main entry point for the GUI application."""
    root = tk.Tk()
    app = ETLGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
