let donors = [];

function addDonor() {
  const donor = {
    name: name.value,
    age: age.value,
    group: group.value.toUpperCase(),
    city: city.value,
    phone: phone.value
  };
  donors.push(donor);
  alert("Donor Registered Successfully!");
}

function searchDonor() {
  const search = document.getElementById("search").value.toUpperCase();
  const list = document.getElementById("list");
  list.innerHTML = "";

  const result = donors.filter(d => d.group === search);

  if (result.length === 0) {
    list.innerHTML = "<li>No Donors Found</li>";
    return;
  }

  result.forEach(d => {
    list.innerHTML += `<li>
      ${d.name} | ${d.group} | ${d.city} | 📞 ${d.phone}
    </li>`;
  });
}
