if (!pipx_available()) {
  install_pipx()
}

if (!benchconnect_available()) {
  install_benchconnect()
}

if (!datalogistik_available()) {
  install_datalogistik()
}
