<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://github.com/asrvsn/ndgpy">
    <img src="logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">ndgpy: serialization-free numeric dataflow graphs for NumPy</h3>

  <p align="center">
    Run parallel event-based numerical processes on a single host by defining NumPy-based data-emitting components, using shared memory instead of serialization. Inspired by <a href="https://en.wikipedia.org/wiki/Transport_triggered_architecture">transport-triggered processing</a> and <a href="https://en.wikipedia.org/wiki/Systolic_array">systolic arrays</a>.
    <br />
    <a href="https://a0s.co/docs/ndgpy"><strong>Explore the docs [coming soon]»</strong></a>
  </p>
</p>



<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary><h2 style="display: inline-block">Table of Contents</h2></summary>
  <ol>
    <li>
      <a href="#about">About</a>
      <ul>
        <li><a href="#features">Features</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
     <li><a href="#gallery">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#references">References</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About

![Product Name Screen Shot][product-screenshot]


### Example: asynchronous LQR controller for cart-pole problem

Inspired by [1]. Suppose we have an inverted pendulum with a mass (also called cart-pole) from which we can take noisy readings over I/O. 
We could perform state estimation & control in the same execution context, but these could be arbitrarily complex -- although here a simple
LQR controller -- and while we're doing so, we might miss out on readings for other critical applications (in this case, logging). 

This problem can be solved succinctly in `ndgpy` by declaring the sensor and controller in different execution contexts, by the dataflow graph
`CartPoleSystem` below.

```python
state_type = np.dtype([('cart_x', np.float64), ('cart_v', np.float64), ('pole_theta', np.float64), ('pole_omega', np.float64)])

class Sensor(Emitter):
  ''' Reads out noisy samples of cart+pole state from environment '''
  def __init__(self):
    super().__init__(state_type)

  async def compute(self):
    ''' Obtain readings asynchronously '''
    self.output['cart_x'] = await sense_cart_x()
    self.output['cart_v'] = await sense_cart_v()
    self.output['pole_theta'] = await sense_pole_theta()
    self.output['pole_omega'] = await sense_pole_omega()

class Observer(Router):
  ''' Estimates true state of the system using a Kalman filter ''' 
  def __init__(self):
    self.F = np.array([[0, 0, 1, 0], [0, 0, 0, 0], [0, 0, 0, 1], [0, 10, 0, 0]]) # Linearized system dynamics
    self.Q = np.eye(4) # Covariance of process noise
    self.R = np.eye(4) # Covariance of observation noise
    self.B = np.array([[0], [1], [0], [1]]) # Control influence
    self.x_t = None # Current state estimate
    self.P_t = np.eye(4) # Current covariance estimate
    super().__init__(state_type)

  async def compute(self, readout, control):
    if self.x_t is None:
      self.x_t = readout
    else:
      # Pretend these computations are really expensive
      K_t = self.P_t@np.linalg.inv(self.R)
      dx_dt = self.F@self.x_t + self.B@control + K_t@(readout - self.x_t)
      dP_dt = self.F@self.P_t + self.P_t@self.F + self.Q - K_t@self.R@K_t.T
      self.x_t += dx_dt * dt
      self.P_t += dP_dt * dt
    self.output[:] = self.x_t[:]

class Controller(Router):
  ''' LQR controller sends outputs via I/O ''' 
  def __init__(self):
    self.F = np.array([[0, 0, 1, 0], [0, 0, 0, 0], [0, 0, 0, 1], [0, 10, 0, 0]]) # Linearized system dynamics
    self.B = np.array([[0], [1], [0], [1]]) # Control influence
    self.Qc = np.eye(4) # Quadratic state cost
    self.Rc = np.eye(1) # Linear input cost
    self.Kc = solve_ricatti(self.F, self.B, self.Qc, self.Rc) # Obtain optimal control gain
    super.__init__([('control', np.float64)])

  async def compute(self, estimate):
    # Pretend these computations are really expensive
    u = -self.Kc@estimate
    self.output['control'] = u
    await apply_control(u)

class Logger(Collector):
  ''' Log the system states ''' 
  async def compute(self, state):
    await write_to_file(state)

class CartPoleSystem(DFG):
  ''' 
  This system avoids missing out on logged readings by running expensive estimation/control operations in separate contexts.
  '''
  async def setup(self):

    # Run sensor & logger on Context 1
    ctx1 = self.new_context()
    sensor = Sensor()
    logger = Logger()
    await self.add([sensor, logger], ctx1)
    await self.connect(sensor, logger)

    # Run observer & controller on Context 2
    ctx2 = self.new_context()
    observer = Observer()
    controller = Controller()
    await self.add([observer, controller], ctx2)
    await self.connect(sensor, observer)
    await self.connect(observer, controller)
    await self.connect(controller, observer)

CartPoleSystem().start()
```

In this system, there is no serialization happening between `Sensor` and `Controller` even though they execute in parallel -- so we can use states of very high dimension without much cost.

### Features

* 


<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple steps.

### Prerequisites

* `conda` (recommended) or python >= 3.7 

This has not been tested in all environments (especially Windows), so please report bugs.

### Installation

* In local environment
```sh
pip install git+git://github.com/asrvsn/ndgpy.git
```
* As a project dependency
```sh
# Add to `requirements.txt`
-e git://github.com/asrvsn/ndgpy.git#egg=ndgpy
```


<!-- USAGE EXAMPLES -->
## Usage

Use this space to show useful examples of how a project can be used. Additional screenshots, code examples and demos work well in this space. You may also link to more resources.

_For more examples, please refer to the [Documentation (coming soon)](https://a0s.co/docs/ndgpy)_

### Example: 

Definition:
```python

```

Result:


<!-- ROADMAP -->
## Roadmap

See the [open issues](https://github.com/asrvsn/ndgpy/issues) for a list of proposed features (and known issues).

* 

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE` for more information.


<!-- ACKNOWLEDGEMENTS -->
## References

* [http://www.cs.cmu.edu/~cga/dynopt/ltr/](http://www.cs.cmu.edu/~cga/dynopt/ltr/)
* []()
* []()
