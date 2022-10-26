import json
import subprocess
from pathlib import Path
from typing import Any, Dict, Generator, List

from benchadapt import BenchmarkResult
from benchadapt.adapters import GeneratorAdapter
from benchadapt.log import log


class ArrowbenchAdapter(GeneratorAdapter):
    """
    An adapter for running arrowbench benchmarks
    """

    def __init__(
        self,
        arrowbench_executable: str,
        result_fields_override: Dict[str, Any] = None,
        result_fields_append: Dict[str, Any] = None,
    ) -> None:
        self.arrowbench = arrowbench_executable

        super().__init__(
            generator=self.run_arrowbench,
            result_fields_override=result_fields_override,
            result_fields_append=result_fields_append,
        )

    def list_benchmarks(self) -> List[Dict[str, Any]]:
        """
        Get list of benchmark commands from arrowbench CLI

        Returns
        -------
        A list of dicts that can be passed to `arrowbench run`
        """
        res = subprocess.run(f"{self.arrowbench} list", shell=True, capture_output=True)
        return json.loads(res.stdout.decode())

    def run_arrowbench(self) -> Generator[BenchmarkResult, None, None]:
        """
        A generator that uses the arrowbench CLI to list available benchmarks,
        then iterate through the list, running each and yielding the result.
        """
        benchmarks = self.list_benchmarks()
        # subset for demo purposes:
        benchmarks = benchmarks[:10]
        for benchmark in benchmarks:
            command = f"{self.arrowbench} run '{json.dumps(benchmark)}'"
            log.info(f"Running `{command}`")
            res = subprocess.run(
                command,
                shell=True,
                capture_output=True,
            )
            dict_result = json.loads(res.stdout.decode())
            result = BenchmarkResult(**dict_result)
            yield result


if __name__ == "__main__":
    adapter = ArrowbenchAdapter(
        arrowbench_executable=Path(__file__).resolve().parent / "arrowbench",
        result_fields_override={"run_reason": "test"},
    )
    for result in adapter.run():
        print(result)
