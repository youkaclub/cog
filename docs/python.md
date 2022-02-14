# Prediction interface reference

This document defines the API of the `cog` Python module, which is used to define the interface for running predictions on your model.

Tip: Run [`cog init`](getting-started-own-model#initialization) to generate an annotated `predict.py` file that can be used as a starting point for setting up your model.

## Contents

- [`BasePredictor`](#basepredictor)
  - [`Predictor.setup()`](#predictorsetup)
  - [`Predictor.predict(**kwargs)`](#predictorpredictkwargs)
- [`Input(**kwargs)`](#inputkwargs)
- [`Output(BaseModel)`](#outputbasemodel)

## `BasePredictor`

You define how Cog runs predictions on your model by defining a class that inherits from `BasePredictor`. It looks something like this:

```python
from cog import BasePredictor, Path, Input
import torch

class Predictor(BasePredictor):
    def setup(self):
        """Load the model into memory to make running multiple predictions efficient"""
        self.model = torch.load("weights.pth")

    def predict(self,
            image: Path = Input(description="Image to enlarge"),
            scale: float = Input(description="Factor to scale image by", default=1.5)
    ) -> Path:
        """Run a single prediction on the model"""
        # ... pre-processing ...
        output = self.model(image)
        # ... post-processing ...
        return output
```

Your Predictor class should define two methods: `setup()` and `predict()`.

### `Predictor.setup()`

Prepare the model so multiple predictions run efficiently.

Use this _optional_ method to include any expensive one-off operations in here like loading trained models, instantiate data transformations, etc.

It's best not to download model weights or any other files in this function. You should bake these into the image when you build it. This means your model doesn't depend on any other system being available and accessible. It also means the Docker image ID becomes an immutable identifier for the precise model you're running, instead of the combination of the image ID and whatever files it might have downloaded.

### `Predictor.predict(**kwargs)`

Run a single prediction.

This _required_ method is where you call the model that was loaded during `setup()`, but you may also want to add pre- and post-processing code here.

The `predict()` method takes an arbitrary list of named arguments, where each argument name must correspond to an [`Input()`](#inputkwargs) annotation.

`predict()` can return strings, numbers, `pathlib.Path` objects, or lists or dicts of those types. You can also define a custom [`Output()`](#outputbasemodel) for more complex return types.

#### Returning `pathlib.Path` objects

If the output is a `pathlib.Path` object, that will be returned by the built-in HTTP server as a file download.

To output `pathlib.Path` objects the file needs to exist, which means that you probably need to create a temporary file first. This file will automatically be deleted by Cog after it has been returned. For example:

```python
def predict(self, image: Path = Input(description="Image to enlarge")) -> Path:
    output = do_some_processing(image)
    out_path = Path(tempfile.mkdtemp()) / "my-file.txt"
    out_path.write_text(output)
    return out_path
```

## `Input(**kwargs)`

Use cog's `Input()` function to define each of the parameters in your `predict()` method:

```py
class Predictor(BasePredictor):
    def predict(self,
            image: Path = Input(description="Image to enlarge"),
            scale: float = Input(description="Factor to scale image by", default=1.5, gt=0, lt=10)
    ) -> Path:
```

The `Input()` function takes these keyword arguments:

- `description`: A description of what to pass to this input for users of the model.
- `default`: A default value to set the input to. If this argument is not passed, the input is required. If it is explicitly set to `None`, the input is optional.
- `gt`: For `int` or `float` types, the value must be greater than this number.
- `ge`: For `int` or `float` types, the value must be greater than or equal to this number.
- `lt`: For `int` or `float` types, the value must be less than this number.
- `le`: For `int` or `float` types, the value must be less than or equal to this number.
- `min_length`: For `str` types, the minimum length of the string.
- `max_length`: For `str` types, the maximum length of the string.
- `regex`: For `str` types, the string must match this regular expression.
- `choices`: A list of possible values for this input.

Each parameter of the `predict()` method must be annotated with a type. The supported types are:

- `str`: a string
- `int`: an integer
- `float`: a floating point number
- `bool`: a boolean
- `cog.File`: a file-like object representing a file
- `cog.Path`: a path to a file on disk

## `Output(BaseModel)`

Your `predict()` method can return a simple data type like a string or a number, or a more complex object with multiple values.

You can optionally use cog's `Output()` function to define the object returned by your `predict()` method:

```py
from cog import BasePredictor, BaseModel

class Output(BaseModel):
    text: str
    file: File

class Predictor(BasePredictor):
    def predict(self) -> Output:
        return Output(text="hello", file=io.StringIO("hello"))
```

## Upgrading from Cog 0.x to 1.x

In Cog versions up to 0.0.20 you described inputs using annotations on your `predict` method. For example:

```py
@cog.input("image", type=Path, help="Image to enlarge")
@cog.input("scale", type=float, default=1.5, help="Factor to scale image by")
def predict(self, image, scale):
    ...
```

From Cog 1.0.0 onwards, we've started using [Pydantic](https://pydantic-docs.helpmanual.io/) to define input and output types. Rather than describing inputs using annotations, you now describe them with type hinting. Here's how you'd define the same inputs now:

```py
def predict(self,
    image: Path = Input(description="Image to enlarge"),
    scale: float = Input(description="Factor to scale image by", default=1.5)
) -> Path:
    ...
```

The parameters that `Input()` takes are pretty similar to those `@cog.input()` used to take. Here are the differences:

- It no longer takes a `type` parameter; use a type hint instead.
- The `help` parameter has been renamed to `description`.
- The `options` parameter has been renamed to `choice`.
- The `min` option has been replaced by two options:
    - `ge`: greater than or equal to (direct replacement)
    - `gt`: greater than (a new alternative)
- The `max` option has been replaced by two options:
    - `le`: less than or equal to (direct replacement)
    - `lt`: less than (a new alternative)

The other major difference is that you now need to define the output type of your model. That's the `-> Path` bit in the example above. That might be a simple type like `str`, `float` or `bool`. If you need to handle multiple outputs, check out the [new documentation for complex output objects](…). If you only have a single output, but it can be of different types depending on the run, you can use `typing.Any`:

```py
from typing import Any

def predict(self) -> Any:
    ...
```
