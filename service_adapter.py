# service_adapter.py
from typing import Any, Tuple
from service_auth import ServiceAuth
from gspread.utils import ValueInputOption
from gspread_formatting import (CellFormat, Color, TextFormat,
                                Borders, Border, format_cell_range,
                                set_column_widths)
from settings import logger

class ServiceAdapter:
    def __init__(self, service_account_file: str, scopes: list[str], sheet_name: str, worksheet_name: str, headers: Tuple[str, ...]):
        self.__service_auth = ServiceAuth(service_account_file, scopes, sheet_name, worksheet_name)
        self._worksheet = self.__service_auth.get_google_sheet()
        self.num_header_rows = 1
        self.headers = headers
        self._set_headers()

    def _set_headers(self):
        try:
            self._worksheet.update([self.headers], value_input_option=ValueInputOption.user_entered)
            header_range = f'A1:{chr(65 + len(self.headers) - 1)}1'  # A1:B1 for two headers, A1:C1 for three, etc.
            self._worksheet.format(header_range, {
                "textFormat": {"bold": True}
            })
            
            logger.debug(f'[ServiceAdapter] Headers set and formatted: {self.headers}')
        except Exception as e:
            logger.error(f'[ServiceAdapter] An error occurred while setting headers: {e}')


class ServiceUpdater(ServiceAdapter):
    def __init__(self, service_account_file: str, scopes: list[str], sheet_name: str, worksheet_name: str, headers: Tuple[str, ...]):
        super().__init__(service_account_file, scopes, sheet_name, worksheet_name, headers)

    def clear(self):
        logger.info('[ServiceUpdater] Write worksheet truncating...')
        try:
            self._worksheet.clear()
            self._set_headers()  # Reset headers after clearing
            self._apply_formatting()
        except Exception as e:
            logger.error(f'[ServiceUpdater] An error occurred while truncating: {e}')

    def append_rows(self, data: list[dict[str, Any]]):
        try:
            rows_to_append = [[d.get(header, '') for header in self.headers] for d in data]
            self._worksheet.append_rows(rows_to_append, value_input_option=ValueInputOption.user_entered)
            logger.debug(f'[ServiceUpdater] Rows appended: {data}')
            self._apply_formatting()
        except Exception as e:
            logger.error(f'[ServiceUpdater] An error occurred while appending rows: {e}')

    def _apply_formatting(self):
        try:
            # Get the number of rows with content
            values = self._worksheet.get_all_values()
            last_row_with_content = len(values)

            # Define formats
            header_format = CellFormat(
                backgroundColor=Color(0.9, 0.9, 0.9),
                textFormat=TextFormat(bold=True),
                horizontalAlignment='CENTER',
                borders=Borders(
                    top=Border(style='SOLID'),
                    bottom=Border(style='SOLID'),
                    left=Border(style='SOLID'),
                    right=Border(style='SOLID')
                )
            )

            body_format = CellFormat(
                backgroundColor=Color(1, 1, 1),
                textFormat=TextFormat(bold=False),
                borders=Borders(
                    top=Border(style='SOLID'),
                    bottom=Border(style='SOLID'),
                    left=Border(style='SOLID'),
                    right=Border(style='SOLID')
                )
            )

            # Apply formats only to rows with content
            format_cell_range(self._worksheet, f'A1:{chr(64 + len(self.headers))}1', header_format)
            if last_row_with_content > 1:
                format_cell_range(self._worksheet, f'A2:{chr(64 + len(self.headers))}{last_row_with_content}', body_format)

            # Set column widths
            set_column_widths(self._worksheet, [('A:' + chr(64 + len(self.headers)), 150)])

            logger.info('[ServiceUpdater] Formatting applied successfully')
        except Exception as e:
            logger.error(f'[ServiceUpdater] An error occurred while applying formatting: {e}')


class ServiceReader(ServiceAdapter):
    def __init__(self, service_account_file: str, scopes: list[str], sheet_name: str, worksheet_name: str, headers: Tuple[str, ...]):
        super().__init__(service_account_file, scopes, sheet_name, worksheet_name, headers)

    def read_columns_generator(self):
        try:
            all_values = self._worksheet.get_all_values()
            for row in all_values[1:]:  # Skip the header row
                if row and row[0].strip():  # Check if the row is not empty and the first cell is not just whitespace
                    logger.debug(f'[ServiceReader] Accepted value: {row}')
                    yield row[0]
                else:
                    logger.debug(f'[ServiceReader] Skipping empty row: {row}')
        except Exception as e:
            logger.error(f'[ServiceReader] An error occurred while reading data: {e}')