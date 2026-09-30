package com.fake.dataworks.controller;

import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.service.DebugParameterService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/runs/parameters")
public class DebugParameterController {
    public record PrepareInput(StudioObject object) {}
    private final DebugParameterService parameters;
    public DebugParameterController(DebugParameterService parameters) {this.parameters=parameters;}
    @PostMapping("/prepare") public DebugParameterService.Preparation prepare(@RequestBody PrepareInput input) {return parameters.prepare(input.object());}
}
