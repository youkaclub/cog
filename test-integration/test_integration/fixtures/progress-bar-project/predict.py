from time import sleep
from tqdm import tqdm

from cog import BasePredictor


class Predictor(BasePredictor):
    def predict(self) -> str:
        print("Starting")
        for i in tqdm(range(2), bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}"):
            sleep(1)
        print("Finished")
        return "output"
