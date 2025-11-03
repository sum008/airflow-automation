class ConnectorBase:

    def fetch_metadata(self, *args, **kwargs):
        """
        Template method to fetch pipeline metadata from source.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def insert_metadata(self, *args, **kwargs):
        """
        Template method to insert pipeline metadata at source.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def update_metadata(self, *args, **kwargs):
        """
        Template method to update pipeline metadata at source.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")
