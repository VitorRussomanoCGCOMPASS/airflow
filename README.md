[![en](https://img.shields.io/badge/lang-en-red.svg)](https://github.com/VitorRussomanoCGCOMPASS/airflow/blob/dev/README.md)
[![pt-br](https://img.shields.io/badge/lang-pt--br-green.svg)](https://github.com/VitorRussomanoCGCOMPASS/airflow/blob/dev/README.pt-br.md)




<style>
img[src*='#left'] {
    float: left;
}
img[src*='#right'] {
    float: right;
}
img[src*='#center'] {
    display: block;
    margin: auto;
}
img[alt=drawing] { width: 500px; }

</style>


<style>
.center {
    text-align: center;
}
</style>

<html>
<head>
<style>
figcaption {
  color: white;
  font-style: italic;
  padding: 2px;
  text-align: center;
}
</style>

# `Airflow`




## `Introduction` : Technical Terms

---
[**Tasks**]


[**DAGS**]


[**Hooks**]


[**Sensors**]


[**Operators**]


<br></br>

## Building the `Code-Base`

The goal of this section is provide an insight on the `workflow` of designing and developing a new `DAG`, along with providing arguments to support our decision to build a custom library of **Operators** and **Sensors** and how did this impact our development `time-line`.

To do that, we start by introducing the concept of _**PythonOperator**_ , which can be easily extended to the notion of _**PythonSensor**_.


<br></br>

### `PythonOperator` x `CustomOperator`
----

    [PythonOperator] 

    Transforms a `Callable` into a Operator instance. This is a quick way of generating a new task, and even though we can arguee that it can be reused, just like any python function, we believe that in the long-term, as the code-base grows and the number of DAGS increase, it can become quite difficult to manage all those imports/dependencies and visualize all the instances of that function.



```python
Example of [PythonOperator]

@task(task_id="print_string")
def print_context(text: str , **kwargs):
    """Print a string"""
    print(str)
    return None
```

    [Custom Operators]

    Alternatively, in a more `Object-Oriented-Programming` manner, we can build classes that represent a single operator, which will in turn allow for the creation of tasks by instances of the object. 
    By encapsulating common or complex tasks within Custom Operators, has a significant positive impact consistency and standardization across the project, while reducing the redudancy, with more ease.


```python
Exemplo de [Custom Operator]

class HelloOperator(BaseOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}"
        print(message)
        return message
```

__Note: On our personal experience, while the  development of Custom Operators may require a longer upfront investment compared to using PythonOperators, the ability to streamline development, ensure consistency, and simplify the integration with external systems has made the project more efficient, maintainable, and scalable__



<br></br>

### `Our-Way`:  `WorkFlow`
------------

In terms of development, we have adopted the following:

1. Identify the process: Clearly define the end-to-end process or workflow.
2. Map the process end-to-end: Break down the identified process into individual tasks or steps that need to be executed. Determine the order of execution and any dependencies between tasks.
3. Check for existing **Operators**: Review the available **Operators** to see if any of them align with the tasks in your process. If the `DAG` can be created with instances of already developed objects, we move forward, if not, we have to make the following assesment:
   1.  Could we add a `feature` to an existing **Operator** that would 
   2.  