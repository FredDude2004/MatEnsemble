# === TODO === 
- [x] Create strategy base class 
- [x] Implement the stategies 
- [x] Create strategy base class for processing futures
- [x] Implement strategies for processing futures
- [x] Refactor matflux.py and matfluxGen.py to be more modular and in one manager.py 

- [x] Test matflux/matfluxGen refactor make sure it works before doing anything else
- [x] Fix problems and test again 
    * Problems: ~/problems.txt
    - [x] Use *ONE* executor in the manager super loop instead of spawning new ones each time
    - [x] Make sure future objects have proper fields appended at creation (task_ or task + job_spec)
    - [x] Move writing of restart files into the FutureProcessingStrategy implementations
    - [x] Make sure you remove the finished future rather than popleft in FutureProcessingStrategy implementations

## NOTE: Refactored code runs way slower 

- [x] Fix problems causing slowdown and test again
    * More Problems: 
    - [x] Make tests consistent so that we have an apples to apples comparison
    - [x] Remove extra logging and RPC calls to limit traffic 
    - [x] Update resources calls to update in place in submit_until_ooresources()

- [x] Test matensemble again until it is working as before

  **Got it working as before**

- [x] Update logging to be more industry standard 
- [x] Refactor Fluxlet to remove global side effects
- [ ] Add type annotations back to strategies
- [ ] Document all of the code vigorously 
    - [x] Document manager.py
    - [ ] Document fluxlet.py
    - [ ] Document strategies/*
- [ ] Remove all TODOs and HACKs

- [ ] Allow tasks to have other tasks as dependencies

