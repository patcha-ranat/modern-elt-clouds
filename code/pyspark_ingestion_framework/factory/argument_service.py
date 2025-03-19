from abstract.argument_service import AbstractArgumentService
from utils.logging import set_logger


class ArgumentService(AbstractArgumentService):
    """
    Validate arguments and log before starting Ingestion
    """
    def __init__(self, args):
        """Class Entrypoint"""
        super().__init__()
        self.logger = set_logger(__class__.__name__)
        self.args = args
    
    def validate(self) -> None:
        """
        Validate arguments
        """
        if self.args.ingestion_mode == "streaming":
            raise Exception("ingestion_mode 'streaming' is not supported yet.")
        if len((self.args.dt).split("-")) != 3:
            raise Exception(f"Please, check if dt is in a literal format of 'YYYY-MM-DD'")
        if ("://" not in self.args.schema_path) or ("://" not in self.args.input_path) or ("://" not in self.args.output_path):
            self.logger.warning("schema_path/input_path/output_path is not in URI format. Assumed local filesystem is utilized.")
        if self.args.format != "delta":
            self.logger.warning(f"Using Output format: {self.args.format}")

    def log(self) -> None:
        """
        Log Arguments
        """
        arguments: dict = {
            "schema_path": self.args.schema_oath,
            "input_path": self.args.input_path,
            "output_path": self.args.output_path,
            "dt": self.args.dt,
            "write_mode": self.args.write_mode,
            "format": self.args.format,
            "ingestion_mode": self.args.ingestion_mode
        }
        self.logger.info(arguments)

    def process(self):
        """Class Main Process"""
        self.validate()
        self.log()
