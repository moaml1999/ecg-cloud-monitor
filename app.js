(() => {
  "use strict";

  const BROKER_URL = "wss://k20a22ef.ala.eu-central-1.emqxsl.com:8084/mqtt";
  const TOPICS = {
    data: "iot-ecg/device01/data",
    bpm: "iot-ecg/device01/bpm",
    control: "iot-ecg/device01/control",
  };
  const BUFFER_SIZE = 1000;

  const ui = {
    canvas: document.getElementById("ecgCanvas"),
    chartWrap: document.getElementById("chartWrap"),
    chartEmpty: document.getElementById("chartEmpty"),
    connectionPill: document.getElementById("connectionPill"),
    connectionText: document.getElementById("connectionText"),
    bpmValue: document.getElementById("bpmValue"),
    bpmNote: document.getElementById("bpmNote"),
    streamState: document.getElementById("streamState"),
    lastPacket: document.getElementById("lastPacket"),
    packetCount: document.getElementById("packetCount"),
    sampleCounter: document.getElementById("sampleCounter"),
    connectButton: document.getElementById("connectButton"),
    startButton: document.getElementById("startButton"),
    stopButton: document.getElementById("stopButton"),
    fullscreenButton: document.getElementById("fullscreenButton"),
    loginDialog: document.getElementById("loginDialog"),
    loginForm: document.getElementById("loginForm"),
    dialogClose: document.getElementById("dialogClose"),
    usernameInput: document.getElementById("usernameInput"),
    passwordInput: document.getElementById("passwordInput"),
    loginButton: document.getElementById("loginButton"),
    formError: document.getElementById("formError"),
  };

  const ctx = ui.canvas.getContext("2d");
  let mqttClient = null;
  let samples = [];
  let expectedSequence = null;
  let packetCount = 0;
  let streaming = false;
  let renderQueued = false;
  let currentScale = 400;

  function setConnection(state, text) {
    ui.connectionPill.dataset.state = state;
    ui.connectionText.textContent = text;
    const online = state === "online";
    ui.startButton.disabled = !online || streaming;
    ui.stopButton.disabled = !online || !streaming;
    ui.connectButton.textContent = online ? "قطع الاتصال" : "الاتصال بالخادم";
  }

  function setStreaming(value) {
    if (!mqttClient?.connected) return;
    mqttClient.publish(TOPICS.control, value ? "start" : "stop", { qos: 0 });
    streaming = value;
    if (ui.streamState) {
      ui.streamState.textContent = value ? "يعمل" : "متوقف";
      ui.streamState.style.color = value ? "var(--green)" : "";
    }
    ui.startButton.disabled = value;
    ui.stopButton.disabled = !value;
  }

  function connectToBroker(username, password) {
    if (!window.mqtt) {
      ui.formError.textContent = "تعذر تحميل مكتبة الاتصال. تحقق من اتصال الإنترنت.";
      return;
    }

    setConnection("connecting", "جارٍ الاتصال");
    ui.loginButton.disabled = true;
    ui.formError.textContent = "";

    const clientId = `ecg-web-${crypto.randomUUID().slice(0, 12)}`;
    mqttClient = mqtt.connect(BROKER_URL, {
      clientId,
      username,
      password,
      clean: true,
      keepalive: 30,
      connectTimeout: 12000,
      reconnectPeriod: 3000,
      protocolVersion: 4,
    });

    mqttClient.on("connect", () => {
      mqttClient.subscribe([TOPICS.data, TOPICS.bpm], { qos: 0 }, (error) => {
        if (error) {
          ui.formError.textContent = "تم الاتصال لكن تعذر الاشتراك في موضوعات البيانات.";
          setConnection("error", "خطأ في الصلاحيات");
          return;
        }
        setConnection("online", "متصل وآمن");
        ui.loginDialog.close();
        ui.passwordInput.value = "";
        ui.loginButton.disabled = false;
        setStreaming(true);
      });
    });

    mqttClient.on("message", handleMessage);
    mqttClient.on("reconnect", () => setConnection("connecting", "إعادة الاتصال"));
    mqttClient.on("offline", () => setConnection("connecting", "الاتصال متوقف"));
    mqttClient.on("close", () => {
      if (mqttClient) setConnection("connecting", "انقطع الاتصال");
    });
    mqttClient.on("error", (error) => {
      const authError = /auth|not authorized|bad user/i.test(error.message || "");
      ui.formError.textContent = authError
        ? "اسم المستخدم أو كلمة المرور غير صحيحة."
        : "تعذر الاتصال بالخادم. تحقق من الإنترنت وبيانات الدخول.";
      setConnection("error", "فشل الاتصال");
      ui.loginButton.disabled = false;
      if (!ui.loginDialog.open) ui.loginDialog.showModal();
    });
  }

  function disconnectBroker() {
    if (!mqttClient) return;
    if (mqttClient.connected) mqttClient.publish(TOPICS.control, "stop", { qos: 0 });
    mqttClient.end(true);
    mqttClient = null;
    streaming = false;
    if (ui.streamState) ui.streamState.textContent = "متوقف";
    setConnection("offline", "غير متصل");
  }

  function handleMessage(topic, payload) {
    if (topic === TOPICS.bpm) {
      updateBpm(Number(payload.toString()));
      return;
    }
    if (topic !== TOPICS.data) return;

    try {
      const frame = JSON.parse(payload.toString());
      if (!Array.isArray(frame.samples)) return;
      appendFrame(frame);
      if (Number.isFinite(Number(frame.bpm))) updateBpm(Number(frame.bpm));
    } catch {
      // Ignore malformed frames without interrupting the live session.
    }
  }

  function appendFrame(frame) {
    let incoming = frame.samples.map(Number).filter(Number.isFinite);
    if (!incoming.length) return;

    const sequence = Number(frame.sequence);
    if (Number.isFinite(sequence)) {
      if (expectedSequence !== null && sequence < expectedSequence) {
        const overlap = expectedSequence - sequence;
        if (overlap >= incoming.length) return;
        incoming = incoming.slice(overlap);
      }
      expectedSequence = sequence + frame.samples.length;
    }

    samples.push(...incoming);
    if (samples.length > BUFFER_SIZE) samples.splice(0, samples.length - BUFFER_SIZE);
    packetCount += 1;

    if (ui.packetCount) ui.packetCount.textContent = packetCount.toLocaleString("ar-IQ");
    ui.sampleCounter.textContent = `${samples.length.toLocaleString("ar-IQ")} عينة`;
    if (ui.lastPacket) {
      ui.lastPacket.textContent = new Intl.DateTimeFormat("ar-IQ", {
        hour: "2-digit", minute: "2-digit", second: "2-digit"
      }).format(new Date());
    }
    ui.chartEmpty.classList.add("hidden");
    requestRender();
  }

  function updateBpm(value) {
    if (!Number.isFinite(value) || value <= 0) return;
    const bpm = Math.round(value);
    ui.bpmValue.textContent = bpm;
    ui.bpmNote.textContent = bpm < 60 ? "أقل من النطاق المعتاد للراحة" : bpm > 100 ? "أعلى من النطاق المعتاد للراحة" : "ضمن النطاق المعتاد للراحة";
  }

  function resizeCanvas() {
    const rect = ui.chartWrap.getBoundingClientRect();
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    ui.canvas.width = Math.max(1, Math.round(rect.width * dpr));
    ui.canvas.height = Math.max(1, Math.round(rect.height * dpr));
    ui.canvas.style.width = `${rect.width}px`;
    ui.canvas.style.height = `${rect.height}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    requestRender();
  }

  function requestRender() {
    if (renderQueued) return;
    renderQueued = true;
    requestAnimationFrame(() => {
      renderQueued = false;
      drawChart();
    });
  }

  function drawChart() {
    const width = ui.canvas.clientWidth;
    const height = ui.canvas.clientHeight;
    if (!width || !height) return;

    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = "#07120f";
    ctx.fillRect(0, 0, width, height);
    drawGrid(width, height);
    if (samples.length < 2) return;

    const sortedAbs = samples.map(Math.abs).sort((a, b) => a - b);
    const percentile = sortedAbs[Math.floor(sortedAbs.length * 0.97)] || 1;
    const targetScale = Math.max(120, percentile * 1.25);
    currentScale += (targetScale - currentScale) * 0.13;

    const paddingX = 18;
    const centerY = height / 2;
    const usableWidth = width - paddingX * 2;
    const amplitude = height * 0.39;
    const startOffset = BUFFER_SIZE - samples.length;

    const gradient = ctx.createLinearGradient(0, 0, width, 0);
    gradient.addColorStop(0, "rgba(89,225,172,.45)");
    gradient.addColorStop(.18, "#59e1ac");
    gradient.addColorStop(1, "#71f0bf");

    ctx.beginPath();
    samples.forEach((value, index) => {
      const x = paddingX + ((startOffset + index) / (BUFFER_SIZE - 1)) * usableWidth;
      const normalized = Math.max(-1, Math.min(1, value / currentScale));
      const y = centerY - normalized * amplitude;
      if (index === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = gradient;
    ctx.lineWidth = 1.65;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    ctx.shadowColor = "rgba(89,225,172,.32)";
    ctx.shadowBlur = 8;
    ctx.stroke();
    ctx.shadowBlur = 0;
  }

  function drawGrid(width, height) {
    ctx.save();
    ctx.strokeStyle = "rgba(142,170,161,.10)";
    ctx.lineWidth = 1;
    const small = Math.max(16, width / 42);
    const large = small * 5;

    for (let x = width % small; x < width; x += small) {
      ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, height); ctx.stroke();
    }
    for (let y = height % small; y < height; y += small) {
      ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(width, y); ctx.stroke();
    }

    ctx.strokeStyle = "rgba(142,170,161,.18)";
    for (let x = width % large; x < width; x += large) {
      ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, height); ctx.stroke();
    }
    for (let y = height % large; y < height; y += large) {
      ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(width, y); ctx.stroke();
    }

    ctx.strokeStyle = "rgba(89,225,172,.20)";
    ctx.beginPath(); ctx.moveTo(0, height / 2); ctx.lineTo(width, height / 2); ctx.stroke();
    ctx.restore();
  }

  ui.connectButton.addEventListener("click", () => {
    if (mqttClient?.connected) disconnectBroker(); else ui.loginDialog.showModal();
  });
  ui.loginForm.addEventListener("submit", (event) => {
    event.preventDefault();
    connectToBroker(ui.usernameInput.value.trim(), ui.passwordInput.value);
  });
  ui.dialogClose.addEventListener("click", () => ui.loginDialog.close());
  ui.startButton.addEventListener("click", () => setStreaming(true));
  ui.stopButton.addEventListener("click", () => setStreaming(false));
  ui.fullscreenButton.addEventListener("click", () => {
    if (document.fullscreenElement) document.exitFullscreen();
    else ui.chartWrap.requestFullscreen?.();
  });
  window.addEventListener("resize", resizeCanvas);
  document.addEventListener("fullscreenchange", () => setTimeout(resizeCanvas, 60));
  window.addEventListener("beforeunload", () => {
    if (mqttClient?.connected) mqttClient.publish(TOPICS.control, "stop", { qos: 0 });
  });

  setConnection("offline", "غير متصل");
  resizeCanvas();
})();

