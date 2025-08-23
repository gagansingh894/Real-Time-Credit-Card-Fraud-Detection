from pydantic.dataclasses import dataclass

@dataclass
class Metrics:
    """
    Metrics is a dataclass for storing metrics related to spark ml jobs

    Attributes:
        precision (float): The precision score of the model.
        recall (float): The recall score of the model.
        f1_score (float): The F1 score of the model, harmonic mean of precision and recall.
        auc (float): The Area Under the Receiver Operating Characteristic curve.
    """

    precision: float
    recall: float
    f1_score: float
    auc: float