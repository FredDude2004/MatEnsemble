=== TODO === 
- [x] Create strategy base class 
- [x] Implement the stategies 
- [x] Create strategy base class for processing futures
- [x] Implement strategies for processing futures

- [x] Test matflux/matfluxGen refactor make sure it works before doing anything else
- [ ] Fix problems and test again 
    * Problems:
    - [ ] Use *ONE* executor in the manager super loop instead of spawning new ones each time
    - [ ] Make sure future objects have proper fields appended at creation (task_ or task + job_spec)
    - [ ] Move writing of restart files into the FutureProcessingStrategy implementations
    - [ ] Make sure you remove the finished future rather than popleft in FutureProcessingStrategy implementations

- [ ] Test matensemble again until it is working as before

- [ ] Update logging to be more industry standard 
- [ ] Refactor Fluxlet to remove global side effects
- [ ] Add type annotations back to strategies
- [ ] Document all of the code vigorously 

- [ ] Allow tasks to have other tasks as dependencies

