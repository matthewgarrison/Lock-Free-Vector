# Lock-Free-Vector

An implementation of Dechev et al.'s lock-free vector from their paper, ["Lock-Free Dynamically Resizable Arrays"](https://www.researchgate.net/publication/225249181_Lock-Free_Dynamically_Resizable_Arrays).

An implementation of Walulya et al.'s lock-free vector from their paper, ["Scalable Lock-Free Vector with Combining"](https://www.researchgate.net/publication/318125515_Scalable_Lock-Free_Vector_with_Combining).

Possible issues:
* Does the ABA problem occur with cached, boxed primitives? eg. for Integers, two numbers will be the same object if their value is on [-128, 127] (and they were autoboxed from ints).
